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

func (self *ExampleExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
    fmt.Println("Call ExampleExecutor.Re-registered")
}

func (self *ExampleExecutor) Disconnected(exec.ExecutorDriver) {
    fmt.Println("Call ExampleExecutor.Disconnected")
}

func (self *ExampleExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
    fmt.Println("Call ExampleExecutor.LaunchTask")

    runStatus := &mesos.TaskStatus{
        TaskId: taskInfo.GetTaskId(),
        State:  mesos.TaskState_TASK_RUNNING.Enum(),
    }
    _, err := driver.SendStatusUpdate(runStatus)
    if err != nil {
        fmt.Println("Got error in ExampleExecutor.LaunchTask: ", err)
    }

    exec.tasksLaunched++

    finStatus := &mesos.TaskStatus{
        TaskId: taskInfo.GetTaskId(),
        State:  mesos.TaskState_TASK_FINISHED.Enum(),
    }
    _, err = driver.SendStatusUpdate(finStatus)
    if err != nil {
        fmt.Println("Got error in ExampleExecutor.LaunchTask: ", err)
    }
}