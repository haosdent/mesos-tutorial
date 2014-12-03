package main

import (
    "fmt"
    exec "github.com/mesos/mesos-go/executor"
    mesos "github.com/mesos/mesos-go/mesosproto"
)

type ExampleExecutor() struct {
    tasksLaunched int
}

func NewExampleExecutor() *ExampleExecutor {
    instance := ExampleExecutor{
        tasksLaunched: 0
    }
    return &instance
}

func (self *ExampleExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwInfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
    fmt.Println("Call ExampleExecutor.Registered")
}