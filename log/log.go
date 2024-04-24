package dlog

import (
	"log"
	"os"
)

var Dlog = log.New(os.Stdout, "", log.LstdFlags)

func Init() {
	Dlog.SetFlags(log.Lmicroseconds | log.Lshortfile)
}
