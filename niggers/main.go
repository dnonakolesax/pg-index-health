package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
)

type SqlConf struct {
	Username   string
	Password   string
	Addr       string
	Port       string
	Dbname     string
	SchemaName string
}

type CheckerConf struct {
	SqlConf
	BloatLimit                   float64
	RemainingPercentageThreshold float64
}

func getEnvRequired(required string) string {
	val, isOk := os.LookupEnv(required)
	if !isOk {
		slog.Error("ENV VARIABLE NOT FOUND: " + required)
	}
	return val
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("EROR LOADING DOTENV FILE: %s", err.Error())
	}
	uname := getEnvRequired("PG_UNAME")
	password := getEnvRequired("PG_PASSWORD")
	dbname := getEnvRequired("PG_DBNAME")
	host := getEnvRequired("PG_HOST")
	port := getEnvRequired("PG_PORT")
	sqlConf := SqlConf{
		Username: uname,
		Password: password,
		Addr:     host,
		Port:     port,
		Dbname:   dbname,
	}
	conf := CheckerConf{
		SqlConf: sqlConf,
	}
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", conf.Username, conf.Password, conf.Addr, conf.Port, conf.Dbname)
	db, err := sqlx.Connect("pgx", connStr)
	db.SetMaxOpenConns(30)
	db.SetMaxIdleConns(0)

	if err != nil {
		log.Fatal(err.Error())
	}
	

	for i := 0; i < 30; i++ {
		rows, err := db.Query("SELECT * FROM bad_table_2 WHERE int_field > 50")

		if err != nil {
			log.Fatal(err.Error())
		}
		rows.Close()

		rows, err = db.Query("INSERT INTO bloat_example(textfield) VALUES ('sampletext')")

		if err != nil {
			log.Fatal(err.Error())
		}
		rows.Close()
	}
}
