package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

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

func getTableChecksums(dsn string) (map[string]int64, error) {
	checksums := make(map[string]int64)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	log.Println("Connected to database" + dsn)
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableName string
	for rows.Next() {
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		log.Print("Checking table " + tableName)
		row := db.QueryRow(fmt.Sprintf("CHECKSUM TABLE %s", tableName))
		var table string
		var checksum int64
		err = row.Scan(&table, &checksum)
		if err != nil {
			return nil, err
		}

		checksums[tableName] = checksum
	}

	return checksums, nil
}

func main() {
	configFile := flag.String("f", "", "Path to the YAML configuration file")
	flag.Parse()
	if *configFile == "" {
		log.Fatal("No configuration file specified")
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

	checksums1, err := getTableChecksums(config.Server1.DSN)
	if err != nil {
		log.Fatalf("Error getting checksums for server 1: %s", err)
	}

	checksums2, err := getTableChecksums(config.Server2.DSN)
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
