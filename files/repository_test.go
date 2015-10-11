package files

import (
	"os"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type RepositorySuite struct{}

var _ = Suite(&RepositorySuite{})

var baseP string

func (s *RepositorySuite) SetUpSuite(c *C) {
	ss, err := os.Getwd()
	if err != nil {
		c.Fatal(err)
	} else {
		baseP = ss
	}
}

func (s *RepositorySuite) TestCanListFilesMatchingProperPattern(c *C) {
	sut := &CqlFileRepo{Path: baseP + "/test_files"}
	if files, err := sut.FindAll(); err != nil {
		c.Fatal(err)
	} else {
		cnt := len(files)
		c.Assert(cnt, Equals, 2)

		c.Assert(files[0].Version, Equals, 1)
		c.Assert(files[0].Description, Equals, "myyoyo")

		c.Assert(files[1].Version, Equals, 3)
		c.Assert(files[1].Description, Equals, "pooyoyo")
	}

}

func (s *RepositorySuite) TestListingFilesWithBadScriptFails(c *C) {
	sut := &CqlFileRepo{Path: baseP + "/bad_scripts"}
	files, err := sut.FindAll()
	c.Assert(files, IsNil)
	c.Assert(err, NotNil)
}

/*
todo:
- can get all files matchin pattern
- does not blow up when no files matched
*/
