all: build run

build:
	go build -buildmode=plugin ./apps/wc.go

run:
	go run main.go wc.so *.txt
