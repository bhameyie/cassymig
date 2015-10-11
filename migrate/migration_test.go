package migration

import (
	"errors"
	"testing"
	"time"

	"github.com/bhameyie/cassymig/files"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MigrationSuite struct{}

var _ = Suite(&MigrationSuite{})

func (s *MigrationSuite) TestShouldPanicWhenRevertApplyFailsOnDown(c *C) {
	script1 := Script{Version: 11, Stmts: []string{"ss"}}

	ser := &FailsApplyOnSecondAttempt{Cnt: 1}
	err := migrateDown(script1, ser)

	c.Assert(err, NotNil)
}

func (s *MigrationSuite) TestShouldPanicWhenRemoveVersionFailsOnDown(c *C) {
	script1 := Script{Version: 11, Stmts: []string{"ss"}}
	ser := &CannotRemoveVersion{}
	err := migrateDown(script1, ser)

	c.Assert(err, NotNil)
}

func (s *MigrationSuite) TestShouldWorkWeelOnDown(c *C) {
	script1 := Script{Version: 11, Stmts: []string{"ss"}}
	ser := &NoOpVersionService{}
	err := migrateDown(script1, ser)

	c.Assert(err, IsNil)
}

func (s *MigrationSuite) TestCanMigrateAllUpWhenAllIsWellOnUp(c *C) {
	noOP := &NoOpVersionService{}
	mig := files.MigrationFile{}
	err := migrateUp(noOP, []files.MigrationFile{mig})
	c.Assert(err, IsNil)
}

func (s *MigrationSuite) TestCanRevertWhenOneApplyChangeFailsOnUp(c *C) {
	ser := &FailsApplyOnSecondAttempt{}
	mig1 := files.MigrationFile{Version: 1, Down: []string{""}}
	mig2 := files.MigrationFile{Version: 2, Down: []string{""}}
	err := migrateUp(ser, []files.MigrationFile{mig1, mig2})
	c.Assert(err, NotNil)
	c.Assert(len(ser.Reverted), Equals, 2)
	c.Assert(ser.Reverted[0], Equals, 2)
	c.Assert(ser.Reverted[1], Equals, 1)
}

func (s *MigrationSuite) TestShouldPanicWhenAddVersionFailsOnUp(c *C) {
	ser := &CannotAddVersion{}
	mig1 := files.MigrationFile{Version: 1, Down: []string{""}}
	mig2 := files.MigrationFile{Version: 2, Down: []string{""}}
	err := migrateUp(ser, []files.MigrationFile{mig1, mig2})
	c.Assert(err, NotNil)
	c.Assert(len(ser.Reverted), Equals, 1)
	c.Assert(ser.Reverted[0], Equals, 1)
}

func (s *MigrationSuite) TestShouldPanicWhenRevertApplyFailsOnUp(c *C) {
	var err interface{}
	defer func() {
		err = recover()
	}()

	stack := &Stack{}
	script1 := Script{Version: 11, Stmts: []string{"ss"}}
	stack.Push(script1)

	ser := &FailsApplyOnSecondAttempt{Cnt: 1}
	revertUpgrades(stack, ser)

	c.Assert(err, NotNil)
}

func (s *MigrationSuite) TestShouldPanicWhenRemoveVersionFailsOnUp(c *C) {
	var err interface{}
	defer func() {
		err = recover()
	}()

	stack := &Stack{}
	script1 := Script{Version: 11, Stmts: []string{"ss"}}
	stack.Push(script1)

	ser := &CannotRemoveVersion{}
	revertUpgrades(stack, ser)

	c.Assert(err, NotNil)
}

//STUBS

type CannotRemoveVersion struct {
	Reverted []int
}

func (s *CannotRemoveVersion) AddVersion(version int, description string, appliedOn time.Time) error {
	return nil
}

func (s *CannotRemoveVersion) RemoveVersion(version int) error {

	return errors.New("AA")
}

func (s *CannotRemoveVersion) ApplyChanges(stmts []string) error {
	return nil
}

type CannotAddVersion struct {
	Reverted []int
}

func (s *CannotAddVersion) AddVersion(version int, description string, appliedOn time.Time) error {
	return errors.New("AA")
}

func (s *CannotAddVersion) RemoveVersion(version int) error {
	s.Reverted = append(s.Reverted, version)

	return nil
}

func (s *CannotAddVersion) ApplyChanges(stmts []string) error {
	return nil
}

type FailsApplyOnSecondAttempt struct {
	Reverted []int
	Cnt      int
}

func (s *FailsApplyOnSecondAttempt) AddVersion(version int, description string, appliedOn time.Time) error {
	return nil
}

func (s *FailsApplyOnSecondAttempt) RemoveVersion(version int) error {
	s.Reverted = append(s.Reverted, version)
	return nil
}

func (s *FailsApplyOnSecondAttempt) ApplyChanges(stmts []string) error {
	s.Cnt++
	if s.Cnt == 2 {
		return errors.New("poof")
	}
	return nil
}

type NoOpVersionService struct {
}

func (s *NoOpVersionService) AddVersion(version int, description string, appliedOn time.Time) error {
	return nil
}

func (s *NoOpVersionService) RemoveVersion(version int) error {
	return nil
}

func (s *NoOpVersionService) ApplyChanges(stmts []string) error {
	return nil
}
