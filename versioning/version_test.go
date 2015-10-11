package versioning

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	. "gopkg.in/check.v1"
)

const (
	host     = "127.0.0.1"
	keyspace = "cassymig"
	username = "cassandra"
	password = "cassandra"
)

func Test(t *testing.T) { TestingT(t) }

type VersioningSuite struct{}

var _ = Suite(&VersioningSuite{})

var session *gocql.Session
var sut = &VersionService{}

func buildSession() (*gocql.Session, error) {
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	return cluster.CreateSession()
}

func (s *VersioningSuite) SetUpSuite(c *C) {
	var err error
	if session, err = buildSession(); err == nil {
		sut.Session = session
		if err := EnsureTableExists(sut.Session); err != nil {
			c.Fatal(err)
		}
	} else {
		c.Fatal(err)
	}
}

func (s *VersioningSuite) TearDownSuite(c *C) {
	if session != nil {
		session.Close()

	}
}

func (s *VersioningSuite) TearDownTest(c *C) {
	if session != nil {
		session.Query("truncate Schema_Version").Exec()
	}
}

func (s *VersioningSuite) TestObtainingCurrentVersiononNewDbIsNegative1(c *C) {
	v, _ := sut.GetCurrentVersion()
	c.Assert(v, Equals, -1)
}

func (s *VersioningSuite) TestCanObtainCurrentVersion(c *C) {
	tt := time.Now()
	t2 := tt.AddDate(-101, 1, 1)
	t3 := tt.AddDate(-200, 1, 1)
	sut.AddVersion(1, "bla", t3)
	sut.AddVersion(8, "bla", t2)
	sut.AddVersion(123, "bla", tt)

	if v, err := sut.GetCurrentVersion(); err == nil {
		c.Assert(v, Equals, 123)
	} else {
		c.Fatal(err)
	}
}

func (s *VersioningSuite) TestCanSuccesfullyRemoveExistingVersion(c *C) {
	tt := time.Now()
	sut.AddVersion(123, "bla", tt)

	err := sut.RemoveVersion(123)
	c.Assert(err, IsNil)
}

func (s *VersioningSuite) TestRemovingNonExistingVersionDoesNotFail(c *C) {
	err := sut.RemoveVersion(123)
	c.Assert(err, IsNil)
}

func (s *VersioningSuite) TestCanGetPreviousVersion_When_Table_Has_SingleVersion_ReturnsNegative(c *C) {
	tt := time.Now()
	sut.AddVersion(123, "bla", tt)

	curr, prev, err := sut.GetPreviousVersion()
	c.Assert(prev, Equals, -1)
	c.Assert(curr, Equals, 123)
	c.Assert(err, IsNil)
}

func (s *VersioningSuite) TestCanGetPreviousVersion_When_Table_NoVersions_ReturnsNegative(c *C) {
	curr, prev, err := sut.GetPreviousVersion()
	c.Assert(prev, Equals, -1)
	c.Assert(curr, Equals, -1)
	c.Assert(err, IsNil)
}

func (s *VersioningSuite) TestCanExecuteBatch(c *C) {
	ss := []string{`INSERT INTO Schema_Version (versionId, appliedOn, description)
		 VALUES (1, dateof(now()), 'dd')`, `INSERT INTO Schema_Version (versionId, appliedOn, description)
	 		 VALUES (2, dateof(now()), 'dd')`,
	}
	err := sut.ApplyChanges(ss)
	c.Assert(err, IsNil)
}

func (s *VersioningSuite) TestCanGetPreviousVersion_When_Table_Is_Populated_ReturnsProperOne(c *C) {
	tt := time.Now()
	t2 := tt.AddDate(-101, 1, 1)
	t3 := tt.AddDate(-200, 1, 1)
	sut.AddVersion(1, "bloom", t3)
	sut.AddVersion(8, "loom", t2)
	sut.AddVersion(123, "lol", tt)

	curr, prev, err := sut.GetPreviousVersion()
	c.Assert(prev, Equals, 8)
	c.Assert(curr, Equals, 123)
	c.Assert(err, IsNil)
}
