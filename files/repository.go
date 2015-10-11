package files

import (
	"errors"
	"io/ioutil"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v2"
)

//MigrationFile represents the content of the igration script
type MigrationFile struct {
	Version     int //The Version Id
	Description string
	Up          []string
	Down        []string
}

//IFileRepo is a repository of migre scripts
type IFileRepo interface {
	FindAll() ([]MigrationFile, error)
}

//CqlFileRepo is an implementation of a file repository
type CqlFileRepo struct {
	Path string
}

type byID []MigrationFile

func (a byID) Len() int           { return len(a) }
func (a byID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byID) Less(i, j int) bool { return a[i].Version < a[j].Version }

//FindAll retrieves all the valid migration scripts
func (repo *CqlFileRepo) FindAll() ([]MigrationFile, error) {
	var migrations []MigrationFile
	tracker := make(map[int]int)

	valids, err := filepath.Glob(repo.Path + "/*.cql")
	if err != nil {
		return nil, err
	}
	for _, s := range valids {
		if content, err := ioutil.ReadFile(s); err == nil {
			mig := MigrationFile{}
			if err := yaml.Unmarshal(content, &mig); err == nil {
				_, ok := tracker[mig.Version]
				if ok {
					return nil, errors.New("Duplicate version found: " + string(mig.Version))
				}
				tracker[mig.Version] = mig.Version
				migrations = append(migrations, mig)
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	sort.Sort(byID(migrations))
	return migrations, nil
}
