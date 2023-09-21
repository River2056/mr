package main

import (
	"mapreducedemo/common"
	"mapreducedemo/model"
	"os"
	"plugin"
)

func main() {
    mapf, reducef := loadPlugin()
    mapf("123", "test")
    reducef("test", []string{})
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
