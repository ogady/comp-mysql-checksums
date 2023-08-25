package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"

	"gopkg.in/yaml.v2"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Server1 struct {
		DSN string `yaml:"dsn"`
	} `yaml:"server1"`
	Server2 struct {
		DSN string `yaml:"dsn"`
	} `yaml:"server2"`
}

type TableInfo struct {
	Name     string
	RowCount int64
}

func getTableChecksums(dsn, schema string, parallelism int) (map[string]int64, error) {
	checksums := make(map[string]int64)
	var mutex sync.Mutex
	conDSN := dsn + "/" + schema
	db, err := sql.Open("mysql", conDSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	log.Println("Connected to database " + conDSN)

	// 変更点: information_schema.TABLESからテーブル情報を取得
	query := fmt.Sprintf("SELECT table_name, table_rows FROM information_schema.TABLES WHERE table_schema = '%s'", schema)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var tableName string
		var rowCount int64
		err := rows.Scan(&tableName, &rowCount)
		if err != nil {
			return nil, err
		}
		tables = append(tables, TableInfo{Name: tableName, RowCount: rowCount})
	}

	// Sort tables by row count
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].RowCount < tables[j].RowCount
	})

	var wg sync.WaitGroup
	chunks := len(tables) / parallelism

	for i := 0; i < parallelism; i++ {
		start := i * chunks
		end := start + chunks
		if i == parallelism-1 {
			end = len(tables)
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			for j := start; j < end; j++ {
				tableName := tables[j].Name

				log.Println("Getting checksum for table " + tableName)
				row := db.QueryRow(fmt.Sprintf("CHECKSUM TABLE %s", tableName))

				var table string
				var checksum int64
				err := row.Scan(&table, &checksum)
				if err != nil {
					log.Printf("Error getting checksum for table %s: %s", tableName, err)
					continue
				}

				mutex.Lock()
				checksums[tableName] = checksum
				mutex.Unlock()
			}
		}(start, end)
	}

	wg.Wait()

	return checksums, nil
}

func main() {
	configFile := flag.String("f", "", "Path to the YAML configuration file")
	parallelism := flag.Int("p", 1, "Number of parallel threads")
	schema := flag.String("s", "", "Schema name")
	flag.Parse()

	if *configFile == "" {
		log.Fatal("Please specify a configuration file")
	}
	if *schema == "" {
		log.Fatal("Please specify a schema name")
	}

	yamlFile, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading YAML file: %s", err)
	}

	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error parsing YAML file: %s", err)
	}

	checksums1, err := getTableChecksums(config.Server1.DSN, *schema, *parallelism)
	if err != nil {
		log.Fatalf("Error getting checksums for server 1: %s", err)
	}

	checksums2, err := getTableChecksums(config.Server2.DSN, *schema, *parallelism)
	if err != nil {
		log.Fatalf("Error getting checksums for server 2: %s", err)
	}

	mismatchedTables := []string{}
	for table, checksum1 := range checksums1 {
		checksum2, ok := checksums2[table]
		if !ok || checksum1 != checksum2 {
			mismatchedTables = append(mismatchedTables, table)
		}
	}

	if len(mismatchedTables) == 0 {
		fmt.Println("All table checksums match.")
	} else {
		fmt.Printf("Mismatched tables: %v\n", mismatchedTables)
	}
}
