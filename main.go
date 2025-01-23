package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	// Define command-line flags
	interval := flag.String("interval", "1m", "Check interval, e.g., 1m, 30s")
	dsn := flag.String("dsn", "", "PostgreSQL DSN")
	table := flag.String("table", "", "Table name to monitor")
	metricName := flag.String("metric", "", "Prometheus metric name")
	outputDir := flag.String("output-dir", "/opt/node-exporter/prom", "Directory to write Prometheus metric files")
	jobName := flag.String("job", "postgres_monitor", "Job name for Pushgateway")
	instance := flag.String("instance", "localhost", "Instance name for Pushgateway")
	flag.Parse()

	if *dsn == "" || *table == "" || *metricName == "" || *outputDir == "" || *jobName == "" || *instance == "" {
		log.Fatal("All parameters  are required.")
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
			rowCount = -1
		}

		// 构建文件路径
		filePath := fmt.Sprintf("%s/%s.prom", *outputDir, *metricName)
		log.Printf("正在写入指标数据到 %s/%s.porm", filePath, *metricName)

		// 使用封装好的函数写文件
		if err := writeToPromFile(filePath, *metricName, *instance, *jobName, rowCount); err != nil {
			log.Printf("写入文件 %s 时出错: %v", filePath, err)
		} else {
			log.Printf("成功写入到 %s", filePath)
		}

		// Wait for the next interval
		time.Sleep(duration)
	}
}

// 封装好的函数，用于写入 Prometheus 格式的数据到文件
func writeToPromFile(filePath string, metric string, instance string, job string, value int) error {
	file, err := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("无法打开文件 %s: %v", filePath, err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(
		file,
		"%s{instance=\"%s\",job=\"%s\"} %d\n",
		metric, instance, job, value,
	)
	if err != nil {
		return fmt.Errorf("写入文件 %s 时发生错误: %v", filePath, err)
	}

	return nil
}
