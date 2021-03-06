package main

import (
    "flag"
    "fmt"
    "io/ioutil"
    "net"
    "log"
    _ "time"
    "net/http"
    "strconv"
    "strings"

    "golang.org/x/net/context"
    "github.com/gogo/protobuf/proto"
    "github.com/mesos/mesos-go/auth"
    "github.com/mesos/mesos-go/auth/sasl"
    "github.com/mesos/mesos-go/auth/sasl/mech"
    mesos "github.com/mesos/mesos-go/mesosproto"
    util "github.com/mesos/mesos-go/mesosutil"
    sched "github.com/mesos/mesos-go/scheduler"
)

const (
    CPUS_PER_TASK       = 1
    MEM_PER_TASK        = 128
    defaultArtifactPort = 12345
)

var (
    address      = flag.String("address", "127.0.0.1", "Binding address for artifact server")
    artifactPort = flag.Int("artifactPort", defaultArtifactPort, "Binding port for artifact server")
    authProvider = flag.String("mesos_authentication_provider", sasl.ProviderName,
        fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
    master              = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
    executorPath        = flag.String("executor", "./executor", "Path to test executor")
    taskCount           = flag.String("task-count", "5", "Total task count to run.")
    mesosAuthPrincipal  = flag.String("mesos_authentication_principal", "", "Mesos authentication principal.")
    mesosAuthSecretFile = flag.String("mesos_authentication_secret_file", "", "Mesos authentication secret file.")
)

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
    fmt.Println("Call ExampleScheduler.Registered")
}

func (self *ExampleScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
    fmt.Println("Call ExampleScheduler.Reregistered")
}

func (self *ExampleScheduler) Disconnected(sched.SchedulerDriver) {
    fmt.Println("Call ExampleScheduler.Disconnected")
}

func (self *ExampleScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
    fmt.Println("Call ExampleScheduler.ResourceOffers")
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

        fmt.Println("Receive offer, Cpu: ", cpus, ", Mem:", mems)

        remainCpus := cpus
        remainMems := mems

        var tasks []*mesos.TaskInfo
        for self.tasksLaunched < self.totalTasks &&
            CPUS_PER_TASK <= remainCpus &&
            MEM_PER_TASK <= remainMems {
            self.tasksLaunched++

            taskId := &mesos.TaskID{
                Value: proto.String(strconv.Itoa(self.tasksLaunched)),
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

        }
        driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
    }
}

func (self *ExampleScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
    fmt.Println("Call ExampleScheduler.StatusUpdate:", status.State.Enum().String())

    if status.GetState() == mesos.TaskState_TASK_FINISHED {
        self.tasksFinished++
    }

    if self.tasksFinished >= self.totalTasks {
        driver.Stop(false)
    }

    if status.GetState() == mesos.TaskState_TASK_LOST ||
       status.GetState() == mesos.TaskState_TASK_KILLED ||
       status.GetState() == mesos.TaskState_TASK_FAILED {
        driver.Abort()
    }
}

func (self *ExampleScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {
    fmt.Println("Call ExampleScheduler.OfferRescinded")
}

func (self *ExampleScheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
    fmt.Println("Call ExampleScheduler.FrameworkMessage")
}

func (self *ExampleScheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {
    fmt.Println("Call ExampleScheduler.SlaveLost")
}

func (self *ExampleScheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
    fmt.Println("Call ExampleScheduler.ExecutorLost")
}

func (self *ExampleScheduler) Error(driver sched.SchedulerDriver, err string) {
    fmt.Println("Call ExampleScheduler.Error")
}

func init() {
    flag.Parse()
    fmt.Println("Initializing the ExampleScheduler...")
}

func serveExecutorArtifact(path string) (*string, string) {
    serveFile := func(pattern string, filename string) {
        http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
            http.ServeFile(w, r, filename)
        })
    }

    pathSplit := strings.Split(path, "/")
    var base string
    if len(pathSplit) > 0 {
        base = pathSplit[len(pathSplit) - 1]
    } else {
        base = path
    }
    serveFile("/" + base, path)

    hostUri := fmt.Sprintf("http://%s:%d/%s", *address, *artifactPort, base)

    return &hostUri, base
}

func prepareExecutorInfo() *mesos.ExecutorInfo {
    executorUris := []*mesos.CommandInfo_URI{}
    uri, executorCmd := serveExecutorArtifact(*executorPath)
    executorUris = append(executorUris, &mesos.CommandInfo_URI{Value: uri, Executable: proto.Bool(true)})
    executorCommand := fmt.Sprintf("./%s", executorCmd)
    fmt.Println("Commond:", executorCommand)

    go http.ListenAndServe(fmt.Sprintf("%s:%d", *address, *artifactPort), nil)

    return &mesos.ExecutorInfo{
        ExecutorId: util.NewExecutorID("default"),
        Name:       proto.String("Test Executor (Go)"),
        Source:     proto.String("go_test"),
        Command: &mesos.CommandInfo{
            Value: proto.String(executorCommand),
            Uris:  executorUris,
        },
    }
}

func parseIP(address string) net.IP {
    addr, err := net.LookupIP(address)
    if err != nil {
        log.Fatal(err)
    }
    if len(addr) < 1 {
        log.Fatalf("Failed to parse IP from address '%v'", address)
    }
    return addr[0]
}

func main() {
    exec := prepareExecutorInfo()

    fwinfo := &mesos.FrameworkInfo{
        User: proto.String(""),
        Name: proto.String("Test Framework (Go)"),
    }

    cred := (*mesos.Credential)(nil)
    if *mesosAuthPrincipal != "" {
        fwinfo.Principal = proto.String(*mesosAuthPrincipal)
        secret, err := ioutil.ReadFile(*mesosAuthSecretFile)
        if err != nil {
            log.Fatal(err)
        }
        cred = &mesos.Credential{
            Principal: proto.String(*mesosAuthPrincipal),
            Secret:    secret,
        }
    }
    bindingAddress := parseIP(*address)
    config := sched.DriverConfig{
        Scheduler:      NewExampleScheduler(exec),
        Framework:      fwinfo,
        Master:         *master,
        Credential:     cred,
        BindingAddress: bindingAddress,
        WithAuthContext: func(ctx context.Context) context.Context {
            ctx = auth.WithLoginProvider(ctx, *authProvider)
            ctx = sasl.WithBindingAddress(ctx, bindingAddress)
            return ctx
        },
    }

    driver, err := sched.NewMesosSchedulerDriver(config)
    if err != nil {
        log.Fatal("Could not create a SchedulerDriver: ", err.Error())
    }
    if stat, err := driver.Run(); err != nil {
        log.Fatalf("Stopped with status %s and error %s \n", stat.String(), err.Error())
    }
}












