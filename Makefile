TEST?=./...
VETARGS?=-asmdecl -atomic -bool -buildtags -copylocks -methods -nilfunc -printf -rangeloops -shift -structtags -unsafeptr

default: test

test:
	go test $(TEST) $(TESTARGS) -timeout=30s -parallel=4

updatedeps:
	go get -u github.com/golang/glog
	go get -u gopkg.in/yaml.v2
	go get -u github.com/gocql/gocql
	go get -u gopkg.in/check.v1
	go get -u github.com/golang/mock/mockgen

release:
	mkdir _release
	go build
	mv cassymig _release/cassymig

vet:
		@go tool vet 2>/dev/null ; if [ $$? -eq 3 ]; then \
			go get golang.org/x/tools/cmd/vet; \
		fi
		@echo "go tool vet $(VETARGS) ."
		@go tool vet $(VETARGS) . ; if [ $$? -eq 1 ]; then \
			echo ""; \
			echo "Vet found suspicious constructs. Please check the reported constructs"; \
			echo "and fix them if necessary before submitting the code for review."; \
			exit 1; \
		fi

clean:
	rm -r _release/
