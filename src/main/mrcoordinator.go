package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"time"

	"6.824/mr"
)

func main() {
	// if len(os.Args) < 2 {
	// 	fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
	// 	os.Exit(1)
	// }
	// m := mr.MakeCoordinator(os.Args[1:], 10)
	files := []string{
		"pg-being_ernest.txt", "pg-dorian_gray.txt",
		"pg-frankenstein.txt", "pg-grimm.txt",
		"pg-huckleberry_finn.txt", "pg-metamorphosis.txt",
		"pg-sherlock_holmes.txt", "pg-tom_sawyer.txt",
	}
	m := mr.MakeCoordinator(files, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
