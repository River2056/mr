package main

import (
	"fmt"
	"mapreducedemo/model"
)

func Map(filename, content string) []model.KeyValue {
    fmt.Println("Map function")

    return []model.KeyValue{}
}

func Reduce(key string, values []string) string {
    fmt.Println("Reduce function")

    return ""
}
