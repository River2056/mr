package common

import "log"

func CheckError(err error) {
	if err != nil {
		log.Fatalf("err occurred %v\n", err)
	}
}
