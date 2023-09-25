package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

func Connect(host string, port int, user, password, dbname, sslmode string) *sql.DB {
	db, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			host,
			port,
			user,
			password,
			dbname,
			sslmode,
		),
	)
	if err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	return db
}
