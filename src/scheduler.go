package main

type ExampleScheduler struct {
    executor      *mesos.ExecutorInfo
    tasksLaunched int
    tasksFinished int
    totalTasks    int
}

func NewExampleScheduler(exec *mesos.ExecutorInfo) *ExampleScheduler {
    total, err := strconv.Atoi(*taskCount)
    if err != nil {
        total = 5
    }
    return &ExampleScheduler{
        executor:      exec,
        tasksLaunched: 0,
        tasksFinished: 0,
        totalTasks:    total,
    }
}

func (self *ExampleScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
    fmt.Printfln("Call ExampleScheduler.Registered")
}

func (self *ExampleScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
    fmt.Printfln("Call ExampleScheduler.Reregistered")
}

func (self *ExampleScheduler) Disconnected(sched.SchedulerDriver) {
    fmt.Printfln("Call ExampleScheduler.Disconnected")
}

