build:
	go build -o bin/mock.exe cmd/mock/main.go &
	go build -o bin/planetscale.exe cmd/planetscale/main.go &
	go build -o bin/turso.exe cmd/turso/main.go &
	go build -o bin/upstash.exe cmd/upstash/main.go
