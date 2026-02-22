package main

import (
	"flag"
	"fmt"
	"os"
)

func getDataBasePath() string {
	path := os.Getenv("DATABASE_PATH")
	if path == "" {
		return "database/data"
	}

	return path
}

func main() {

	get := func(kv KV, args []string) {
		cmd := flag.NewFlagSet("get", flag.ExitOnError)

		if err := cmd.Parse(args); err != nil {
			fmt.Printf("error: %s\n", err)
			return
		}

		orgs := cmd.Args()

		if len(orgs) < 1 {
			fmt.Printf("expect more data: %v\n", orgs)
			return
		}

		key := orgs[0]
		val, ok, err := kv.Get([]byte(key))
		if !ok {
			fmt.Printf("get error: %v\n", err)
		}
		fmt.Printf("key:%s, value:%s\n", key, string(val))
	}

	set := func(kv KV, args []string) {
		cmd := flag.NewFlagSet("set", flag.ContinueOnError)

		if err := cmd.Parse(args); err != nil {
			fmt.Printf("error: %s\n", err)
			return
		}

		orgs := cmd.Args()

		if len(orgs) < 2 {
			fmt.Printf("expect more data: %v", orgs)
			return
		}

		key := orgs[0]
		val := orgs[1]

		kv.Set([]byte(key), []byte(val))

		fmt.Printf("key: %s value: %s\n", key, val)
	}

	del := func(kv KV, args []string) {
		cmd := flag.NewFlagSet("delete", flag.ExitOnError)

		if err := cmd.Parse(args); err != nil {
			fmt.Printf("error: %s\n", err)
			return
		}

		orgs := cmd.Args()

		if len(orgs) < 1 {
			fmt.Printf("expect more data: %v\n", orgs)
			return
		}

		key := orgs[0]
		if ok, err := kv.Del([]byte(key)); !ok {
			fmt.Printf("delete error:  %v\n", err)
		}

		fmt.Println("delete success!")
	}

	list := func(kv KV, _ []string) {
		data, err := kv.List()
		if err != nil {
			fmt.Printf("list error: %v", err)
			return
		}

		fmt.Println(string(data))
	}

	database := KV{}
	database.log.FileName = getDataBasePath()

	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		fmt.Printf("expect more data size:%d\n", len(args))
		return
	}

	database.Open()
	defer database.Close()

	switch args[0] {
	case "get":
		get(database, args[1:])
	case "set":
		set(database, args[1:])
	case "delete":
		del(database, args[1:])
	case "list":
		list(database, args[1:])
	default:
		fmt.Println("error")
	}

}
