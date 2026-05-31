.PHONY: test
test:
	go test ./...

.PHONY: cover
cover:
	go test -cover ./...

.PHONY: coveralls
coveralls:
	go test -covermode=atomic -coverprofile=coverage.out ./...

.PHONY: coverhtml
coverhtml:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out