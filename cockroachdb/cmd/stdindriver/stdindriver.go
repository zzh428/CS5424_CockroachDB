package main

import (
	"log"
	"os"
	"strings"

	"github.com/alecthomas/kingpin"

	"cs5424/cockroachdb/db"
)

var (
	userName    = kingpin.Flag("user", "user name").Default("root").String()
	database    = kingpin.Flag("database", "database name").Default("wholesale").String()
	endpointStr = kingpin.Flag("endpoints", "endpoint1,endpoint2,...").Required().String()
)

func main() {
	kingpin.Parse()

	endpoints := strings.Split(*endpointStr, ",")
	driver, err := db.NewDriver(*userName, endpoints[0], *database, os.Stdin)
	if err != nil {
		log.Fatalf("new db driver failed: %v", err)
	}
	defer driver.Stop()

	driver.Run()
}
