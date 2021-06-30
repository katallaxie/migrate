package migrate

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
)

// SqlxMigrate ...
type SqlxMigrate struct {
	Migrations []SqlxMigration

	rw   sync.Mutex
	once sync.Once
}

// New ...
func New() *SqlxMigrate {
	return &SqlxMigrate{}
}

// Add ...
func (s *SqlxMigrate) Add(m SqlxMigration) {
	m.id = len(s.Migrations)
	s.Migrations = append(s.Migrations, m)
}

// Run ...
func (s *SqlxMigrate) Run(sqlDB *sqlx.DB, dialect string) error {
	db := sqlx.NewDb(sqlDB, dialect)

	s.rw.Lock()
	defer s.rw.Unlock()

	if err := s.createMigrationTable(db); err != nil {
		return err
	}

	for _, m := range s.Migrations {
		var found int
		err := db.Get(&found, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" ( "%s" INTEGER )`, s.TableName(), s.ColumnName()), m.ID)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("looking up migration by id: %w", err)
		}

		err = s.runMigration(db, m)
		if err != nil {
			return err
		}
	}

	return nil
}

// TableName ...
func (s *SqlxMigrate) TableName() string {
	return "migrations"
}

// ColumnName ...
func (s *SqlxMigrate) ColumnName() string {
	return "version"
}

func (s *SqlxMigrate) createMigrationTable(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" ( "%s" INTEGER )`, s.TableName(), s.ColumnName()))
	if err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	return nil
}

func (s *SqlxMigrate) run(db *sqlx.DB, m SqlxMigration) error {
	errorf := func(err error) error { return fmt.Errorf("running migration: %w", err) }

	tx, err := db.Beginx()
	if err != nil {
		return errorf(err)
	}
	_, err = db.Exec("INSERT INTO migrations (id) VALUES ($1)", m.ID)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}

	err = m.Migrate(tx)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}

	err = tx.Commit()
	if err != nil {
		return errorf(err)
	}

	return nil
}

func (s *SqlxMigrate) selectVersion(db *sql.DB) (int, error) {
	var row struct {
		Version int
	}

	_, err := db.Exec(fmt.Sprintf(`SELECT "%s" FROM "%s" LIMIT 1`, s.ColumnName(), s.TableName()), row.Version)
	if err != nil {
		return -1, fmt.Errorf("creating migrations table: %w", err)
	}

	return row.Version, nil
}

func (s *SqlxMigrate) insertVersion(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO "%s" ( "%s" ) VALUES ( $1 )`, s.TableName(), s.ColumnName()))
	if err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	return nil
}

func (s *SqlxMigrate) updateVersion(db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf(`UPDATE "%s" SET "%s" = $1 WHERE "%s" = $2`, s.TableName(), s.ColumnName(), s.ColumnName()))
	if err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	return nil
}

// SqlxMigration ...
type SqlxMigration struct {
	ID   int
	Up   func(tx *sqlx.Tx) error
	Down func(tx *sqlx.Tx) error
}

// QueryMigration ...
func QueryMigration(id, upQuery, downQuery string) SqlxMigration {
	queryFn := func(query string) func(tx *sqlx.Tx) error {
		if query == "" {
			return nil
		}

		return func(tx *sqlx.Tx) error {
			_, err := tx.Exec(query)
			return err
		}
	}

	m := SqlxMigration{
		Up:   queryFn(upQuery),
		Down: queryFn(downQuery),
	}

	return m
}
