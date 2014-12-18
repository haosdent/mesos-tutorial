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
        for self.tasksLaunched < self.totalTasks
          && CPUS_PER_TASK <= remainCpus
          && MEM_PER_TASK <= remainMems {
            self.tasksLaunched++

            taskId := &mesos.TaskID{
                Value: proto.String(strconv.Itoa(self.tasksLaunched))
            }

            task := &mesos.TaskInfo{
                Name: proto.String("go-task-" + taskId.GetValue()),
                TaskId: taskId,
                SlaveId: offer.SlaveId,
                Executor: self.executor,
                Resources: []*mesos.Resource{
                    util.NewScalarResource("cpus", CPUS_PER_TASK),
                    util.NewScalarResource("mem", MEM_PER_TASK),
                },
            }

            tasks = append(tasks, task)
            remainMems -= MEM_PER_TASK
            remainCpus -= CPUS_PER_TASK

            driver.LaunchTasks([]*memos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
        }
    }
}

func (self *ExampleScheduler) StatusUpdate(driver self.SchedulerDriver, status *mesos.TaskStatus) {
    fmt.Printfln("Call ExampleScheduler.StatusUpdate")

    if status.GetState() == mesos.TaskState_TASK_FINISHED {
        self.tasksFinished++
    }

    if self.tasksFinished >= self.totalTasks {
        driver.Stop(false)
    }

    if status.GetState() == mesos.TaskState_TASK_LOST
      || status.GetState() == mesos.TaskState_TASK_KILLED
      || status.GetState() == mesos.TaskState_TASK_FAILED {
        driver.Abort()
    }
}

func (self *ExampleScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {
    fmt.Printfln("Call ExampleScheduler.OfferRescinded")
}

func (self *ExampleScheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.OfferID) {
    fmt.Printfln("Call ExampleScheduler.FrameworkMessage")
}

func (self *ExampleScheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {
    fmt.Printfln("Call ExampleScheduler.SlaveLost")
}

func (self *ExampleScheduler) ExecutorLost(sched.SchedulerDriver, *mesos.SlaveID) {
    fmt.Printfln("Call ExampleScheduler.ExecutorLost")
}

func (self *ExampleScheduler) Error(driver sched.SchedulerDriver, err string) {
    fmt.Printfln("Call ExampleScheduler.Error")
}


















