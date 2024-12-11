package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

func main() {
	// Define command-line flags
	interval := flag.String("interval", "1m", "Check interval, e.g., 1m, 30s")
	dsn := flag.String("dsn", "", "PostgreSQL DSN")
	table := flag.String("table", "", "Table name to monitor")
	metricName := flag.String("metric", "", "Prometheus metric name")
	pushGateway := flag.String("pushgateway", "", "Pushgateway address")
	jobName := flag.String("job", "postgres_monitor", "Job name for Pushgateway")
	flag.Parse()

	if *dsn == "" || *table == "" || *metricName == "" || *pushGateway == "" {
		log.Fatal("All parameters (dsn, table, metric, pushgateway) are required.")
	}

	// Parse the interval
	duration, err := time.ParseDuration(*interval)
	if err != nil {
		log.Fatalf("Invalid interval format: %v", err)
	}

	// Create a Prometheus gauge metric
	rowCountMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: *metricName,
		Help: fmt.Sprintf("Row count for table %s", *table),
	})

	// Register the metric with Prometheus
	registry := prometheus.NewRegistry()
	err = registry.Register(rowCountMetric)
	if err != nil {
		log.Fatalf("Failed to register metric: %v", err)
	}

	// Open the database connection
	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	log.Printf("Starting monitoring with interval %s", *interval)

	for {
		// Query the row count
		var rowCount int
		query := fmt.Sprintf("SELECT count(*) FROM %s", *table)
		err := db.QueryRow(query).Scan(&rowCount)
		if err != nil {
			log.Printf("Failed to query row count: %v", err)
		} else {
			// Update the metric
			rowCountMetric.Set(float64(rowCount))

			// Push the metric to Pushgateway
			err = push.New(*pushGateway, *jobName).
				Collector(rowCountMetric).
				Grouping("job", *jobName).
				Push()
			if err != nil {
				log.Printf("Failed to push metrics: %v", err)
			} else {
				log.Printf("Pushed metrics successfully: %s = %d", *metricName, rowCount)
			}
		}

		// Wait for the next interval
		time.Sleep(duration)
	}
}
