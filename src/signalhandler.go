package main

import (
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"os"
	"os/signal"
	"syscall"
	"fmt"
)

func doSignalHandler(goroutine goroutine_mgr.Goroutine, args ...interface{}) {
	defer goroutine.OnQuit()
	for {
		signalChan := make(chan os.Signal)
		signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		signal := <- signalChan
		fmt.Println("catch signal: ", signal)
		quitFlag = true
		break
	}
}

func startSignalHandler() uint64 {
	return goroutineMgr.GoroutineCreatePn("signalhandler", doSignalHandler, nil)
}