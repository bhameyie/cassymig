package migration

import (
	"cassymig/files"
	"cassymig/versioning"
	"fmt"
	"time"
)

//MigrateDown reverts an existing schema to the previously know version
func MigrateDown(repo files.IFileRepo,
	service versioning.IManageVersions) error {

	migFiles, err := repo.FindAll()
	if err != nil {
		return err
	}

	currentV, err := service.GetCurrentVersion()
	if err != nil {
		return err
	}

	if currentV > 0 {
		s := find(currentV, migFiles)
		return migrateDown(s, service)
	}

	return nil
}

//MigrateUp updates the schema to the latest based on available changes
func MigrateUp(repo files.IFileRepo,
	service versioning.IManageVersions) error {

	migFiles, err := repo.FindAll()
	if err != nil {
		return err
	}

	currentV, err := service.GetCurrentVersion()
	if err != nil {
		return err
	}

	migratables := getMigratable(currentV, migFiles)

	migCnt := len(migratables)
	if migCnt > 0 {
		fmt.Printf("Migrating %d files...\n", migCnt)
		return migrateUp(service, migratables)
	}

	return nil
}

func find(v int, migs []files.MigrationFile) Script {
	for _, mig := range migs {
		if mig.Version == v {
			return Script{Version: mig.Version, Stmts: mig.Down}
		}
	}
	return Script{}
}

func getMigratable(v int, migs []files.MigrationFile) []files.MigrationFile {

	for index, mig := range migs {
		if v == mig.Version {
			nxt := index + 1
			if nxt > len(migs) {
				return []files.MigrationFile{}
			}
			return migs[nxt:]
		}
	}
	return migs
}

func migrateDown(n Script, service versioning.IMaintainVersions) error {
	fmt.Printf("Downgrading from version: %d \n", n.Version)
	if err := service.ApplyChanges(n.Stmts); err != nil {
		return err
	}
	if err := service.RemoveVersion(n.Version); err != nil {
		return err
	}
	return nil
}

func revertUpgrades(s *Stack, service versioning.IMaintainVersions) {
	n := s.Pop()
	for n.Stmts != nil {
		if err := migrateDown(n, service); err != nil {
			panic(err)
		}
		n = s.Pop()
	}
}

func migrateUp(service versioning.IMaintainVersions,
	migratables []files.MigrationFile) error {
	stack := &Stack{}
	incrementalInterval := time.Duration(int64(0))
	for _, m := range migratables {
		script := Script{Version: m.Version, Stmts: m.Down}
		stack.Push(script)

		fmt.Printf("Upgrading to version: %d \n", m.Version)
		err := service.ApplyChanges(m.Up)
		if err != nil {
			revertUpgrades(stack, service)
			return err
		}
		aon := time.Now().Add(incrementalInterval)
		ad := service.AddVersion(m.Version, m.Description, aon)
		if ad != nil {
			revertUpgrades(stack, service)
			return ad
		}

		incrementalInterval += time.Second //prevents it from having the same time, which throws the cluster sort
	}
	return nil
}
