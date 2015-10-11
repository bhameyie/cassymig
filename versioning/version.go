package versioning

import (
	"sort"
	"time"

	"github.com/gocql/gocql"
)

//IManageVersions manages interactions with the Schema Info table
type IManageVersions interface {
	GetPreviousVersion() (int, int, error)
	GetCurrentVersion() (int, error)
	ApplyChanges(stmts []string) error
	AddVersion(version int, description string, appliedOn time.Time) error
	RemoveVersion(version int) error
}

//IRecallVersions can retrieve current and previous version from the info table
type IRecallVersions interface {
	GetPreviousVersion() (int, int, error)
	GetCurrentVersion() (int, error)
}

//IMaintainVersions performs update to the schema and its info table
type IMaintainVersions interface {
	ApplyChanges(stmts []string) error
	AddVersion(version int, description string, appliedOn time.Time) error
	RemoveVersion(version int) error
}

type versionItem struct {
	ID        int
	AppliedOn time.Time
}

type byTime []versionItem

func (a byTime) Len() int           { return len(a) }
func (a byTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTime) Less(i, j int) bool { return a[i].AppliedOn.Unix() > a[j].AppliedOn.Unix() }

//VersionService is a concrete implementation of IManageVersions
type VersionService struct {
	Session *gocql.Session
}

//EnsureTableExists makes sure the schema version table is present
func EnsureTableExists(session *gocql.Session) error {
	return session.Query(`
		CREATE TABLE IF NOT EXISTS Schema_Version(
			appliedOn timestamp,
			versionId bigint,
			description text,
			PRIMARY KEY ( versionId, appliedOn )
			)
			WITH CLUSTERING ORDER BY (appliedOn DESC)
		`).Exec()
}

//RemoveVersion inserts a new updated version of the schema to the info table
func (vs *VersionService) RemoveVersion(version int) error {
	if err := vs.Session.
		Query(`DELETE FROM Schema_Version Where versionId = ?`, version).
		Exec(); err != nil {
		return err
	}
	return nil
}

//AddVersion inserts a new updated version of the schema to the info table
func (vs *VersionService) AddVersion(version int, description string, appliedOn time.Time) error {
	if err := vs.Session.Query(`INSERT INTO Schema_Version (versionId, appliedOn, description) VALUES (?, ?, ?)`,
		version, appliedOn, description).Exec(); err != nil {
		return err
	}
	return nil
}

//ApplyChanges execute cql script
func (vs *VersionService) ApplyChanges(stmts []string) (err error) {
	for _, s := range stmts {
		//todo: need to find a transactional way of doing this if one exists
		if err = vs.Session.Query(s).Exec(); err != nil {
			return
		}
	}
	return
}

func getSorted(session *gocql.Session) ([]versionItem, error) {
	var versions []versionItem
	var version int
	var on time.Time

	iter := session.Query("select versionId, appliedOn from Schema_Version").Iter()
	for iter.Scan(&version, &on) {
		versions = append(versions, versionItem{ID: version, AppliedOn: on})
	}
	sort.Sort(byTime(versions))

	return versions, iter.Close()
}

//GetCurrentVersion returns the current version of the database schema
func (vs *VersionService) GetCurrentVersion() (int, error) {
	vers, err := getSorted(vs.Session)
	if len(vers) < 1 {
		return -1, err
	}
	return vers[0].ID, err
}

//GetPreviousVersion returns the previous version of the database schema
func (vs *VersionService) GetPreviousVersion() (int, int, error) {
	vers, err := getSorted(vs.Session)
	s := len(vers)

	if s == 1 {
		return vers[0].ID, -1, err
	}
	if s > 1 {
		return vers[0].ID, vers[1].ID, err
	}
	return -1, -1, err
}
