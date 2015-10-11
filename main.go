package main

import (
	"flag"
	"fmt"

	"github.com/bhameyie/cassymig/files"
	"github.com/bhameyie/cassymig/migrate"
	"github.com/bhameyie/cassymig/versioning"

	"github.com/gocql/gocql"
)

func buildSession(host, keyspace, username, password string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	return cluster.CreateSession()
}

func main() {
	directionPtr := flag.String("direction", "up", "up or down")
	folderPtr := flag.String("source", ".", "scripts folder")
	uriPtr := flag.String("uri", "localhost", "CASSANDRA")
	kspacePtr := flag.String("kspace", "service", "CASSANDRA keyspace")
	userPtr := flag.String("user", "cassandra", "CASSANDRA user")
	passPtr := flag.String("pass", "cassandra", "CASSANDRA pass")

	flag.Parse()
	fmt.Println("Cassymig")
	fmt.Println("")

	session, err := buildSession(*uriPtr, *kspacePtr, *userPtr, *passPtr)
	if err != nil {
		panic(err)
	}
	if err := versioning.EnsureTableExists(session); err != nil {
		panic(err)
	}

	defer session.Close()

	service := &versioning.VersionService{Session: session}
	repo := &files.CqlFileRepo{Path: *folderPtr}

	switch *directionPtr {
	case "up":
		if err := migration.MigrateUp(repo, service); err != nil {
			fmt.Println(err)
			panic(err)
		}
	case "down":
		if err := migration.MigrateDown(repo, service); err != nil {
			fmt.Println(err)
			panic(err)
		}
	default:
		panic("unrecognized mode: " + *directionPtr)
	}
	fmt.Println("Success!!")
}
