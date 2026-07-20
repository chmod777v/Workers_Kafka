package migrator

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
)

type Migrator struct {
	migrate *migrate.Migrate
}

func NewMigrator(migrationsDir, dbLink string) *Migrator {
	m, err := migrate.New(
		migrationsDir,
		dbLink)
	if err != nil {
		panic(err)
	}
	return &Migrator{m}
}

func (m *Migrator) Close() {
	m.migrate.Close()
}

//

func (m *Migrator) Down() bool {
	if err := m.migrate.Down(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			fmt.Println("no migrations to apply")
			return false
		}
		panic(err)
	}
	return true
}

func (m *Migrator) Up() bool {
	if err := m.migrate.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			fmt.Println("no migrations to apply")
			return false
		}
		panic(err)
	}
	return true
}

func (m *Migrator) Steps(value int) bool {
	err := m.migrate.Steps(value)
	if err != nil {
		fmt.Printf("Steps failed: %v\n", err)
		return false
	}
	return true
}

func (m *Migrator) Force() bool {
	version, dirty, err := m.migrate.Version()
	if err != nil {
		fmt.Printf("Cannot get status: %v\n", err)
		return false
	}
	if !dirty {
		fmt.Println("Dirty: false")
		return false
	}

	err = m.migrate.Force(int(version) - 1)
	if err != nil {
		fmt.Printf("Force failed: %v\n", err)
		return false
	}
	return true
}

func (m *Migrator) Status() {
	version, dirty, err := m.migrate.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			fmt.Println("Status: No migrations applied")
			return
		}
		fmt.Printf("Cannot get status: %v\n", err)
		return
	}
	fmt.Printf("- Version: %d\n- Dirty: %v\n", version, dirty)
}
