package main

import (
	"fmt"
	"io/ioutil"
	"mapreducedemo/common"
	"mapreducedemo/model"
	"os"
	"path/filepath"
	"plugin"
	"strings"
	"sync"
)

const (
	OutputPath   string = "output/"
	Intermediate string = "intermediate/"
)

func prepareDirectories() {
	// clear all output first
	os.RemoveAll(OutputPath)
	os.RemoveAll(Intermediate)
	os.Mkdir(OutputPath, os.ModePerm)
	os.Mkdir(Intermediate, os.ModePerm)
}

func main() {
	mapf, reducef := loadPlugin()
	inputs := os.Args[2:]

	basePath, err := os.Getwd()
	common.CheckError(err)

	// Map
	mapValues := make(chan []model.KeyValue)
	var wg sync.WaitGroup
	for _, file := range inputs {
		wg.Add(1)
		filePath := fmt.Sprintf("%v/%v", basePath, file)
		bytes, err := ioutil.ReadFile(filePath)
		common.CheckError(err)
		go func() {
			defer wg.Done()
			res := mapf(filePath, string(bytes))
			mapValues <- res
		}()
	}

	go func() {
		wg.Wait()
		close(mapValues)
	}()

	prepareDirectories()

	idx := 0
	for value := range mapValues {
		outputName := fmt.Sprintf("%v%v.map", Intermediate, idx)
		outputValue := make([]string, 0)
		for _, keyvalue := range value {
			outputValue = append(outputValue, fmt.Sprintf("%v,%v", keyvalue.Key, keyvalue.Value))
		}

		ioutil.WriteFile(outputName, []byte(strings.Join(outputValue, "\n")), os.ModePerm)
		idx++
	}

	// Reduce
	reduceValues := make(chan string)
	combine := make(map[string][]string)
	filepath.Walk(Intermediate, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			common.CheckError(err)
			bytes, err := ioutil.ReadFile(path)
			common.CheckError(err)

			content := string(bytes)
			arr := strings.Split(content, "\n")
			for _, row := range arr {
				keyValueArr := strings.Split(row, ",")
				var arrayInMap []string
				if _, ok := combine[keyValueArr[0]]; !ok {
					arrayInMap = make([]string, 0)
				} else {
					arrayInMap = combine[keyValueArr[0]]
				}
				arrayInMap = append(arrayInMap, keyValueArr[1])
				combine[keyValueArr[0]] = arrayInMap
			}
		}
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for k, v := range combine {
			reduceValues <- reducef(k, v)
		}
	}()

	go func() {
		wg.Wait()
		close(reduceValues)
	}()

	result := make([]string, 0)
	for v := range reduceValues {
		result = append(result, v)
	}

	ioutil.WriteFile(fmt.Sprintf("%v%v", OutputPath, "reduce_output"), []byte(strings.Join(result, "\n")), os.ModePerm)
}

func loadPlugin() (func(string, string) []model.KeyValue, func(string, []string) string) {
	appPluginName := os.Args[1]
	appPlugin, err := plugin.Open(appPluginName)
	common.CheckError(err)

	xmapf, err := appPlugin.Lookup("Map")
	common.CheckError(err)
	mapf := xmapf.(func(string, string) []model.KeyValue)

	xreducef, err := appPlugin.Lookup("Reduce")
	common.CheckError(err)
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
