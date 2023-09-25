package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/cherryReptile/dbtool/internal/config"
	"github.com/cherryReptile/dbtool/internal/postgres"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"strconv"
	"strings"
)

type creds struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}

type info struct {
	tableName     string
	columnName    string
	columnDefault string
}

var (
	cfg             string
	sequencesRepeat string
)

func init() {
	rootCmd.AddCommand(pgCmd)

	pgCmd.Flags().StringVarP(
		&cfg,
		"config",
		"c",
		"",
		`path to config dir. Config like {
				"postgres": {
				"host": "localhost",
				"port": 5432,
				"user": "user",
				"password": "user",
				"sslmode": "disable"
				}
			}`,
	)
	pgCmd.Flags().StringVarP(
		&sequencesRepeat,
		"sequence-repeat",
		"s",
		"",
		"Find sequence repeats for schema. For example: -sr=users",
	)
}

var pgCmd = &cobra.Command{
	Use:   "pg",
	Short: "If need postgres",
	Long:  "Command that will work with postgresql(queries, tables, etc)",
	Run:   runPG,
}

func runPG(cmd *cobra.Command, args []string) {
	var db *sql.DB

	if cfg != "" {
		if err := config.ReadConfig(cfg); err != nil {
			log.Fatalf("failed to read config: %v", err)
		}

		db = postgres.Connect(
			viper.GetString("postgres.host"),
			viper.GetInt("postgres.port"),
			viper.GetString("postgres.user"),
			viper.GetString("postgres.password"),
			viper.GetString("postgres.dbname"),
			viper.GetString("postgres.sslmode"),
		)
	} else {
		credentials, err := getCreds()
		if err != nil {
			log.Fatalf("failed to read creadentials from input: %v", err)
		}

		db = postgres.Connect(
			credentials.host,
			credentials.port,
			credentials.user,
			credentials.password,
			credentials.dbname,
			"disable",
		)
	}

	if sequencesRepeat != "" {
		if err := printRepeats(db, sequencesRepeat); err != nil {
			log.Fatalf("failed to print: %v", err)
		}
	}
}

func getCreds() (*creds, error) {
	var (
		credentials creds
		err         error
	)

	if credentials.host, err = paramReader("please specify host: "); err != nil {
		return nil, err
	}

	portStr, err := paramReader("please specify port: ")
	if err != nil {
		return nil, err
	}

	if credentials.port, err = strconv.Atoi(portStr); err != nil {
		return nil, err
	}

	if credentials.user, err = paramReader("please specify user: "); err != nil {
		return nil, err
	}

	if credentials.password, err = paramReader("please specify password: "); err != nil {
		return nil, err
	}

	if credentials.dbname, err = paramReader("please specify db name: "); err != nil {
		return nil, err
	}

	return &credentials, nil
}

func paramReader(msg string) (string, error) {
	var (
		input string
		try   int
	)

	for input == "" {
		try++

		if try > 3 {
			return "", errors.New("too much tries")
		}

		fmt.Print(msg)

		if _, err := fmt.Scanln(&input); err != nil {
			if err.Error() == "unexpected newline" {
				continue
			}

			return "", err
		}
	}

	return input, nil
}

func printRepeats(db *sql.DB, schema string) error {
	usages, err := findSequences(db, schema)
	if err != nil {
		log.Fatalf("failed to find sequences: %v", err)
	}

	var matches []info

	result := fmt.Sprintf("\nMatches:")

	for i := range usages {
		seqMatches, err := findRepeats(db, usages[i], schema)
		if err != nil {
			return err
		}

		var lastSeq string

		for j := range seqMatches {
			if lastSeq != seqMatches[j].columnDefault {
				result += "\n"
			}

			result += fmt.Sprintf(
				"sequence: %s)\ttableName: %s;\tcolumnName: %s;\n",
				seqMatches[j].columnDefault,
				seqMatches[j].tableName,
				seqMatches[j].columnName,
			)

			matches = append(matches, seqMatches[j])

			lastSeq = seqMatches[j].columnDefault
		}
	}

	if len(matches) == 0 {
		fmt.Printf("No repeats found for %s schema", schema)

		return nil
	}

	fmt.Println(
		fmt.Sprintf(
			"%s\nTotal:%d",
			result,
			len(matches),
		),
	)

	return nil
}

func findSequences(db *sql.DB, schema string) ([]info, error) {
	query := fmt.Sprintf(`
	select table_name, column_name, column_default
		from information_schema.columns
	where column_default like 'nextval%s' and table_schema = '%s'
	`, "%", schema)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	var finds []info

	for rows.Next() {
		var seq info

		if err = rows.Scan(&seq.tableName, &seq.columnName, &seq.columnDefault); err != nil {
			return nil, err
		}

		seq.columnDefault = strings.ReplaceAll(seq.columnDefault, "'", "''")

		finds = append(finds, seq)
	}

	return finds, nil
}

func findRepeats(db *sql.DB, seq info, schema string) ([]info, error) {
	query := fmt.Sprintf(`
		select table_name, column_name, column_default
			from information_schema.columns
		where column_default = '%s' and table_schema = '%s';
		`,
		seq.columnDefault,
		schema,
	)
	rowsSequences, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	var rowsCount int

	var (
		repeatsSequences []info
		firstSequence    info
	)

	for rowsSequences.Next() {
		rowsCount++

		if rowsCount == 1 {
			if err = rowsSequences.Scan(&firstSequence.tableName, &firstSequence.columnName, &firstSequence.columnDefault); err != nil {
				return nil, err
			}

			continue
		}

		if rowsCount > 1 {
			var dupSeq info
			if err = rowsSequences.Scan(&dupSeq.tableName, &dupSeq.columnName, &dupSeq.columnDefault); err != nil {
				return nil, err
			}

			if rowsCount == 2 {
				repeatsSequences = append(repeatsSequences, firstSequence, dupSeq)

				continue
			}

			repeatsSequences = append(repeatsSequences, dupSeq)
		}
	}

	return repeatsSequences, nil
}
