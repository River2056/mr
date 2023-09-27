package main

import (
	"fmt"
	"mapreducedemo/model"
	"strings"
	"unicode"
)

func Map(filename, content string) []model.KeyValue {
    fmt.Println("Map function")

    kva := make([]model.KeyValue, 0)
    words := strings.FieldsFunc(content, func(r rune) bool {return !unicode.IsLetter(r)})

    for _, w := range words {
        kv := model.KeyValue{Key: w, Value: "1"}
        kva = append(kva, kv)
    }

    return kva
}

func Reduce(key string, values []string) string {
    fmt.Println("Reduce function")
    return fmt.Sprintf("%v,%v", key, len(values))
}
