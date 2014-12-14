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

func (self *ExampleScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
    fmt.Printfln("Call ExampleScheduler.ResourceOffers")
    for _, offer := range offers {
        cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
            return res.GetName() == "cpus"
        })
        cpus := 0.0
        for _, res := range cpuResources {
            cpus += res.GetScalar().GetValue()
        }

        memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
            return res.GetName() == "mem"
        })
        mems := 0.0
        for _, res := range memResources {
            mems += res.GetScalar().GetValue()
        }

        fmt.Printfln("Receive offer, Cpu: ", cpus, ", Mem:", mems)

        remainCpus := cpus
        remainMems := mems

        var tasks []*mesos.TaskInfo

    }
}





















