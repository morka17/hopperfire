package main

import (
	"fmt"
	"log"

	"github.com/morka17/hopperDB/hopper"
)

func main() {
	db, err := hopper.New()
	if err != nil {
		log.Fatal(err)
	}

	user := map[string]string{
		"name": "Peace",
		"age": "20",
	}  

	_, err = db.CreateCollection("users")
	if err != nil {
		log.Fatal(err)
	}

	result, err := db.Insert("users", user)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", result)
	
	
}
