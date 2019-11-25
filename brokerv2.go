package main

import (
        "crypto/rand"
        "encoding/csv"
        "fmt"
        "io"
        "io/ioutil"
        "log"
        "os"
        "strconv"
        "strings"
        "sync"
        "time"

        appsv1beta2 "k8s.io/api/apps/v1beta2"
        batchv1 "k8s.io/api/batch/v1"
        metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/client-go/kubernetes"
        _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
        "k8s.io/client-go/tools/clientcmd"

        v1 "k8s.io/api/core/v1"
        "k8s.io/apimachinery/pkg/api/resource"

        "bufio"
        "os/exec"
)

func GetKubeClient(confPath string) *kubernetes.Clientset {

        loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: confPath}
        loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})

        clientConfig, err := loader.ClientConfig()
        if err != nil {
                panic(err)
        }

        kubeclient, err := kubernetes.NewForConfig(clientConfig)
        if err != nil {
                panic(err)
        }
        return kubeclient
}

func tokenGenerator() string {
        b := make([]byte, 6)
        rand.Read(b)
        return fmt.Sprintf("%x", b)
}

func check(e error) {
        if e != nil {
                panic(e)
        }
}

func dump(info, file string) {

        f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
        check(err)
        defer f.Close()

        w := bufio.NewWriter(f)

        w.WriteString(info)
        w.Flush()
}

var (
        wg sync.WaitGroup
        layout    = "2006-01-02 15:04:05 +0000 UTC"
        clientset = GetKubeClient("/root/admin.conf")
)

func main() {

        start := time.Now()
        argsWithoutProg := os.Args[1:]

        inputFile := string(argsWithoutProg[0])
        var experimentDuration = 150 //time.Duration(150) * time.Second

        fmt.Printf("INPUTFILE: %s\n", inputFile)
        fmt.Printf("SECONDS: %s\n", argsWithoutProg[1])

        if len(argsWithoutProg) > 1 {
                experimentDurationInt, _ := strconv.Atoi(argsWithoutProg[1])
                experimentDuration = experimentDurationInt //time.Duration(experimentDurationInt) * time.Second
        }

        var timeRef = 0
        file, err := ioutil.ReadFile(inputFile)

        if err == nil {

                r := csv.NewReader(strings.NewReader(string(file)))

                for {

                        //time.Sleep(time.Duration(1) * time.Second)
                        //time.Sleep(time.Duration(500) * time.Millisecond)
                        record, err := r.Read()

                        if err == io.EOF {
                                break
                        }
                        if err != nil {
                                log.Fatal(err)
                        }

                        if string(record[0]) != "jid" {

                                timestamp, _ := strconv.Atoi(string(record[0]))
                                slo := string(record[11])
                                cpuReq := string(record[8])
                                memReq := string(record[9])
                                taskID := string(record[1])
                                class := string(record[10])

                                controllerKind := string(record[12])
                                replicaOrParallelism := string(record[13])
                                completions := string(record[14])
                                qosMeasuring := string(record[15])

                                var qosMeasuringAux string
                                if qosMeasuring == "time_aggregated" {
                                    qosMeasuringAux = "timeaggregated"
                                } else if qosMeasuring == "task_aggregated" {
                                    qosMeasuringAux = "taskaggregated"
                                } else {
                                    qosMeasuringAux = qosMeasuring
                                }

                                controllerName := class + "-" + controllerKind  + "-" + qosMeasuringAux + "-" + taskID + "-" + tokenGenerator()
                                dump(controllerName+"\n", "controllers.csv")



                                replicaOrParallelismInt, _ := strconv.Atoi(replicaOrParallelism)
                                replicaOrParallelismInt32 := int32(replicaOrParallelismInt)

                                completionsInt, _ := strconv.Atoi(completions)
                                completionsInt32 := int32(completionsInt)



                                if controllerKind == "deployment" {
                                        //port, _ := strconv.Atoi(string(record[16]))
                                        nodePort,_ := strconv.Atoi(string(record[16]))

                                        deployment := getDeploymentSpec(controllerName, cpuReq, memReq, slo, class, &replicaOrParallelismInt32, qosMeasuring) // int32(port))

                                        service := getServiceOfDeployment(deployment,  int32(nodePort))
                                        fmt.Println("Reading new task...")
                                        fmt.Printf("Deployment %s, cpu: %v, mem: %v, slo: %s, replicas: %d, qosMeasuring: %s", controllerName, cpuReq, memReq, slo, replicaOrParallelism, qosMeasuring)

                                        if timestamp == timeRef {
                                                fmt.Println("Time: ", timestamp)
                                                fmt.Println("Creating deployment ", controllerName)
                                                wg.Add(1)
                                                go CreateAndManageDeploymentTermination(controllerName, deployment, service, experimentDuration-timeRef, &wg)

                                        } else {
                                                waittime := int(timestamp - timeRef)
                                                timeRef = timestamp
                                                fmt.Println("")
                                                time.Sleep(time.Duration(waittime) * time.Second)
                                                fmt.Println("Time: ", timestamp)
                                                fmt.Println("Creating deployment ", controllerName)
                                                wg.Add(1)
                                                go CreateAndManageDeploymentTermination(controllerName, deployment, service, experimentDuration-timeRef, &wg)
                                        }

                                } else if controllerKind == "job" {
                                        job := getJobSpec(controllerName, cpuReq, memReq, slo, class, &replicaOrParallelismInt32, &completionsInt32, qosMeasuring)

                                        fmt.Println("Reading new task...")
                                        fmt.Printf("Job %s, cpu: %v, mem: %v, slo: %s, paralelism: %d, completions: %d, qosMeasuring: %s", controllerName, cpuReq, memReq, slo, replicaOrParallelism, completions, qosMeasuring)

                                        if timestamp == timeRef {
                                                fmt.Println("Time: ", timestamp)
                                                fmt.Println("Creating job ", controllerName)
                                                wg.Add(1)
                                                go CreateAndManageJobTermination(controllerName, job, experimentDuration-timeRef, &wg)

                                        } else {
                                                waittime := int(timestamp - timeRef)
                                                timeRef = timestamp
                                                fmt.Println("")
                                                time.Sleep(time.Duration(waittime) * time.Second)
                                                fmt.Println("Time: ", timestamp)
                                                fmt.Println("Creating job ", controllerName)
                                                wg.Add(1)
                                                go CreateAndManageJobTermination(controllerName, job, experimentDuration-timeRef, &wg)
                                        }
                                }


                        }
                }
        }

        wg.Wait()

        elapsed := time.Since(start)
        fmt.Println("Finished - runtime: ", elapsed)
}

func CreateAndManageDeploymentTermination(controllerName string, deployment *appsv1beta2.Deployment, service *v1.Service, expectedRuntime int, wg *sync.WaitGroup) {

        _, err := clientset.AppsV1beta2().Deployments("default").Create(deployment)
        if err != nil {
                fmt.Printf("[ERROR] While creating deployment: ", err)
        }


        _, err = clientset.CoreV1().Services("default").Create(service)
        if err != nil {
                fmt.Printf("[ERROR] While creating service: ", err)
        }

        time.Sleep(time.Duration(expectedRuntime) * time.Second)

        fmt.Println("Killing all deployments and respective services after ", expectedRuntime, "seconds")
        out := fmt.Sprintf("Killing all deployments and services after %s", expectedRuntime)
        dump(out, "/root/broker.log")

        //clientset.AppsV1beta2().Deployments("default").Delete(controllerName, &metav1.DeleteOptions{})
        cmd := exec.Command("/usr/bin/kubectl", "delete", "deploy", controllerName)
        cmd.Run()


        cmd = exec.Command("/usr/bin/kubectl", "delete", "service", service.Name)
        cmd.Run()

        wg.Done()
}

func CreateAndManageJobTermination(controllerName string, job *batchv1.Job, expectedRuntime int, wg *sync.WaitGroup) {

        _, err := clientset.BatchV1().Jobs("default").Create(job)
        if err != nil {
                fmt.Printf("[ERROR] Error while creating job information: ", err)
        }

        time.Sleep(time.Duration(expectedRuntime) * time.Second)

        fmt.Println("Killing all jobs after ", expectedRuntime, "seconds")
        out := fmt.Sprintf("Killing all jobs  after %s", expectedRuntime)
        dump(out, "/root/broker.log")

        cmd := exec.Command("/usr/bin/kubectl", "delete", "job", controllerName)
        cmd.Run()

        wg.Done()
}


func getDeploymentSpec(controllerRefName string,
        cpuReq string, memReq string, slo string, class string,
        replicaOrParallelism *int32, qosMeasuring string) *appsv1beta2.Deployment {

        memReqFloat, _ := strconv.ParseFloat(memReq, 64)
        memReqKi := memReqFloat * 1000000
        memReqStr := strconv.FormatFloat(memReqKi, 'f', -1, 64)
        memRequest := memReqStr + "Ki"
        fmt.Println(memRequest)
        rl := v1.ResourceList{v1.ResourceName(v1.ResourceMemory): resource.MustParse(memRequest),
                v1.ResourceName(v1.ResourceCPU): resource.MustParse(cpuReq)}

        priorityClass := class + "-priority"
        gracePeriod := int64(0)



        pod := v1.PodTemplateSpec{
                ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "nginx", "controller": controllerRefName}, Annotations: map[string]string{"slo": slo, "controller": controllerRefName, "qos_measuring": qosMeasuring}},
                //ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "golang"}, Annotations: map[string]string{"slo": slo, "controller": controllerRefName, "qos_measuring": qosMeasuring}},
                //ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "couchbase"}, Annotations: map[string]string{"slo": slo, "controller": controllerRefName, "qos_measuring": qosMeasuring}},
                Spec: v1.PodSpec{
                        TerminationGracePeriodSeconds: &gracePeriod,
                        Containers: []v1.Container{{Name: controllerRefName,
                                Image: "nginx:1.15",
                                //Image: "golang:1.11",
                                //Image: "couchbase:6.0.1",
                                //Ports: []v1.ContainerPort{{ContainerPort:port}},
                                // TODO this command property must be used only for golang and couchbase deploys
                                //Command: []string{"/bin/bash", "-ce", "tail -f /dev/null" },
                                Resources: v1.ResourceRequirements{
                                        Limits:   rl,
                                        Requests: rl,
                                }}},
                        //},
                        //ImagePullPolicy: v1.PullAlways}},

                        PriorityClassName: priorityClass},
        }


        deployment := &appsv1beta2.Deployment{
                        ObjectMeta: metav1.ObjectMeta{Name: controllerRefName},
                        Spec:       appsv1beta2.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: pod.Labels}, Replicas: replicaOrParallelism, Template: pod},
                }


        fmt.Print("Image Pull Police: ")

        imagePullPoliceOfDeployment := deployment.Spec.Template.Spec.Containers[0].ImagePullPolicy

        if imagePullPoliceOfDeployment == "" {
                fmt.Println("Policy Default !")
        } else {
                fmt.Println(imagePullPoliceOfDeployment)
        }

        return deployment
}


func getJobSpec(controllerRefName string,
        cpuReq string, memReq string, slo string, class string,
        replicaOrParallelism *int32, completions *int32, qosMeasuring string) *batchv1.Job {

        memReqFloat, _ := strconv.ParseFloat(memReq, 64)
        memReqKi := memReqFloat * 1000000
        memReqStr := strconv.FormatFloat(memReqKi, 'f', -1, 64)
        memRequest := memReqStr + "Ki"
        fmt.Println(memRequest)
        rl := v1.ResourceList{v1.ResourceName(v1.ResourceMemory): resource.MustParse(memRequest),
                v1.ResourceName(v1.ResourceCPU): resource.MustParse(cpuReq)}

        priorityClass := class + "-priority"
        gracePeriod := int64(0)

        podSpec := v1.PodTemplateSpec{
                ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "nginx"}, Annotations: map[string]string{"slo": slo, "controller": controllerRefName, "qos_measuring": qosMeasuring}},
                //ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "golang"}, Annotations: map[string]string{"slo": slo, "controller": controllerRefName, "qos_measuring": qosMeasuring}},
                //ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "couchbase"}, Annotations: map[string]string{"slo": slo, "controller": controllerRefName, "qos_measuring": qosMeasuring}},
                Spec: v1.PodSpec{
                        TerminationGracePeriodSeconds: &gracePeriod,
                        Containers: []v1.Container{{Name: controllerRefName,
                                Image: "nginx:1.15",
                                //Image: "golang:1.11",
                                //Image: "couchbase:6.0.1",
                                Command: []string{"sleep", "1800" },
                                Resources: v1.ResourceRequirements{
                                        Limits:   rl,
                                        Requests: rl,
                                }}},
                        //},
                        //ImagePullPolicy: v1.PullAlways}},
                        PriorityClassName: priorityClass,
                        RestartPolicy: "Never"},
        }


        job := &batchv1.Job{
                ObjectMeta: metav1.ObjectMeta{Name: controllerRefName, Namespace: "default"},

                Spec: batchv1.JobSpec{
                        //Selector: &metav1.LabelSelector{MatchLabels: pod.Labels},
                        Template: podSpec,
                        Parallelism: replicaOrParallelism,
                        Completions: completions,
                },

        }

        job.Spec.Template.Labels = map[string]string{"app": "nginx"}
        //job.Spec.Template.Labels = map[string]string{"app": "golang"}
        //job.Spec.Template.Labels = map[string]string{"app": "couchbase"}

        fmt.Print("Image Pull Police: ")
        imagePullPoliceOfDeployment := job.Spec.Template.Spec.Containers[0].ImagePullPolicy

        if imagePullPoliceOfDeployment == "" {
                fmt.Println("Policy Default !")
        } else {
                fmt.Println(imagePullPoliceOfDeployment)
        }

        return job
}



func getServiceOfDeployment(deployment *appsv1beta2.Deployment, nodePort int32) *v1.Service {
        servicePort := v1.ServicePort{Port: 80, NodePort: nodePort}
        serviceSpec := v1.ServiceSpec{
                Ports: []v1.ServicePort{servicePort},
                Selector: deployment.Spec.Template.Labels,
                Type: v1.ServiceTypeNodePort,
        }


        service := &v1.Service{
                ObjectMeta: metav1.ObjectMeta{Name: deployment.Name + "-service", Namespace: "default"},
                Spec: serviceSpec}
        return service
}

