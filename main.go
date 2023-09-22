package main

import (
	"fmt"
	"io/ioutil"
	"mapreducedemo/common"
	"mapreducedemo/model"
	"os"
	"plugin"
	"sync"
)

func main() {
	mapf, reducef := loadPlugin()
	inputs := os.Args[2:]
    fmt.Println(reducef)

	basePath, err := os.Getwd()
	common.CheckError(err)
    
    ch := make(chan []model.KeyValue)
    var wg sync.WaitGroup
    for _, file := range inputs {
        wg.Add(1)
        filePath := fmt.Sprintf("%v/%v", basePath, file)
        bytes, err := ioutil.ReadFile(filePath)
        common.CheckError(err)
        go func() {
            defer wg.Done()
            res := mapf(filePath, string(bytes))
            ch <- res
        }()
    }

    go func() {
        wg.Wait()
        close(ch)
    }()

    fmt.Println("after wait")
    for s := range ch {
        fmt.Println(s)
    }

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
