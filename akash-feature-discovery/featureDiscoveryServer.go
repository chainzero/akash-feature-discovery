package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	v1 "akash-api/v1"
)

const (
	nvidiaVendorID = "10de"
	pciDevicesDir  = "/sys/bus/pci/devices/"
	jsonURL        = "https://gist.githubusercontent.com/chainzero/279e737f85d71084725ffde7821a9084/raw/c7878123bf8cc8646798fedc80f17ba08494f601/akashGpuDatabase.json"

	gpuResourceName     = "nvidia.com/gpu"
	cpuResourceName     = "cpu"
	memResourceName     = "memory"
	storageResourceName = "ephemeral-storage"
)

var currentNodeData v1.Node

type GPUInfo struct {
	Vendor    string `json:"vendor"`
	Name      string `json:"name"`
	ModelID   string `json:"modelid"`
	Interface string `json:"interface"`
	Memory    string `json:"memory"`
}

type SerializableResourcePair struct {
	Allocatable string `json:"allocatable"`
	Allocated   string `json:"allocated"`
}

type SerializableGPU struct {
	Quantity *SerializableResourcePair `json:"quantity"`
}

func getNodeIntel() *v1.Node {
	node := &v1.Node{} // Initialize the node

	// Discover CPU resources
	node.CPU = *discoverCPUs()

	resp, err := http.Get(jsonURL)
	if err != nil {
		fmt.Println(err)
		return node
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Println("non-200")
		return node
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return node
	}

	var gpuInfos []GPUInfo
	if err := json.Unmarshal(body, &gpuInfos); err != nil {
		fmt.Println(err)
		return node
	}

	// Match GPUs based on device IDs
	matchGPUsByDeviceID(node, gpuInfos)

	nodeAllocatableDiscovery(node)

	return node
}

func matchGPUsByDeviceID(node *v1.Node, gpuInfos []GPUInfo) {
	files, err := ioutil.ReadDir(pciDevicesDir)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, f := range files {
		vendorFilePath := filepath.Join(pciDevicesDir, f.Name(), "vendor")
		vendorContent, err := ioutil.ReadFile(vendorFilePath)
		if err != nil {
			continue
		}

		if containsVendorID(string(vendorContent), nvidiaVendorID) {
			deviceFilePath := filepath.Join(pciDevicesDir, f.Name(), "device")
			deviceContent, err := ioutil.ReadFile(deviceFilePath)
			if err != nil {
				continue
			}

			deviceID := strings.TrimSpace(string(deviceContent))[2:] // Remove the "0x" prefix and trim spaces

			for _, gpuInfo := range gpuInfos {
				if gpuInfo.ModelID == deviceID {
					gpu := &v1.GPUInfo{
						Vendor:    gpuInfo.Vendor,
						Name:      gpuInfo.Name,
						ModelID:   gpuInfo.ModelID,
						Interface: gpuInfo.Interface,
						Memory:    gpuInfo.Memory,
					}
					node.Gpus.Info = append(node.Gpus.Info, gpu)
					break
				}
			}
		}
	}
}

func containsVendorID(content, vendorID string) bool {
	trimmedContent := strings.TrimSpace(content)
	return strings.HasSuffix(trimmedContent, vendorID)
}

func discoverCPUs() *v1.CPU {
	cpu := &v1.CPU{}

	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		fmt.Println("Error opening /proc/cpuinfo:", err)
		return cpu
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading /proc/cpuinfo:", err)
		return cpu
	}

	currentCPU := &v1.CPUInfo{}
	priorPhysicalID := "999999"
	currentPhysicalID := "1111111"

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)

		if len(parts) < 2 {
			// Conditional necessary to handle AMD processor which lists individual cores as a unique ID in /proc/cpuinfo
			// Intel processors group cores within each physical socket entry in /proc/cpuinfo
			if currentPhysicalID != priorPhysicalID {
				cpu.Info = append(cpu.Info, currentCPU)
			}
			// Used to skip instances if the CPU current physical ID equals prior physical ID
			// Some processors list each individual core as a separate instance in /proc/cpuinfo, and this variable is used to skip over such redundant entries
			priorPhysicalID = currentPhysicalID
			currentCPU = &v1.CPUInfo{}
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "processor":
			currentCPU.ID = value
		case "vendor_id":
			currentCPU.Vendor = value
		case "cpu cores":
			if val, err := strconv.ParseUint(value, 10, 32); err == nil {
				currentCPU.Vcores = uint32(val)
			} else {
				fmt.Printf("Error converting %s to uint32: %v\n", value, err)
			}
		case "model name":
			currentCPU.Model = value
		case "physical id":
			currentPhysicalID = value
		}
	}

	// Append the last CPU instance
	if currentPhysicalID != priorPhysicalID {
		cpu.Info = append(cpu.Info, currentCPU)
	}

	return cpu
}

type msgServiceServer struct {
	v1.UnimplementedMsgServer
}

func (s *msgServiceServer) QueryNode(empty *v1.VoidNoParam, stream v1.Msg_QueryNodeServer) error {

	for {
		node := getNodeIntel()

		node2 := v1.Node{}
		node2.CPU.Info = node.CPU.Info
		node2.Gpus.Info = node.Gpus.Info
		node2.Memory.Info = node.Memory.Info
		node2.Storage.Info = node.Storage.Info

		// CPU Logic to deal with unexported fields in Kubernetes API machinery API > Quantity struct

		// Check if QuantityLocal is nil and initialize if necessary
		if node2.CPU.QuantityLocal == nil {
			node2.CPU.QuantityLocal = &v1.ResourcePairLocal{}
		}

		allocatableStrCpu := node.CPU.Quantity.Allocatable.String()
		node2.CPU.QuantityLocal.Allocatable = allocatableStrCpu
		allocatatedStrCpu := node.CPU.Quantity.Allocated.String()
		node2.CPU.QuantityLocal.Allocated = allocatatedStrCpu

		// GPU Logic to deal with unexported fields in Kubernetes API machinery API > Quantity struct

		// Check if QuantityLocal is nil and initialize if necessary
		if node2.Gpus.QuantityLocal == nil {
			node2.Gpus.QuantityLocal = &v1.ResourcePairLocal{}
		}

		allocatableStrGpu := node.Gpus.Quantity.Allocatable.String()
		node2.Gpus.QuantityLocal.Allocatable = allocatableStrGpu
		allocatatedStrGpu := node.Gpus.Quantity.Allocated.String()
		node2.Gpus.QuantityLocal.Allocated = allocatatedStrGpu

		// Memory Logic to deal with unexported fields in Kubernetes API machinery API > Quantity struct

		// Check if QuantityLocal is nil and initialize if necessary
		if node2.Memory.QuantityLocal == nil {
			node2.Memory.QuantityLocal = &v1.ResourcePairLocal{}
		}

		allocatableStrMem := node.Memory.Quantity.Allocatable.String()
		node2.Memory.QuantityLocal.Allocatable = allocatableStrMem
		allocatatedStrMem := node.Memory.Quantity.Allocated.String()
		node2.Memory.QuantityLocal.Allocated = allocatatedStrMem

		// Storage Logic to deal with unexported fields in Kubernetes API machinery API > Quantity struct

		// Check if QuantityLocal is nil and initialize if necessary
		if node2.Storage.QuantityLocal == nil {
			node2.Storage.QuantityLocal = &v1.ResourcePairLocal{}
		}

		allocatableStrStorage := node.Storage.Quantity.Allocatable.String()
		node2.Storage.QuantityLocal.Allocatable = allocatableStrStorage
		allocatatedStrStorage := node.Storage.Quantity.Allocated.String()
		node2.Storage.QuantityLocal.Allocated = allocatatedStrStorage

		// Check if data collected has updated and only send to gRPC stream if there is an update
		fmt.Println("currentNodeData: ", currentNodeData)
		fmt.Println("node2: ", node2)
		if !reflect.DeepEqual(currentNodeData, node2) {

			currentNodeData = node2 // Update the current data with the new data

			// Send stream to subscribed gRPC clients
			fmt.Printf("Data sent to gRPC server stream: %+v\n", node2)
			if err := stream.Send(&node2); err != nil {
				return err
			}
		} else {
			fmt.Println("No changes detected in node data.")
		}

		// Sleep or wait for some event before sending the next node
		time.Sleep(5 * time.Second)
	}
}

func getAllocatedResourceForNode(clientset *kubernetes.Clientset, nodeName string, resourceName corev1.ResourceName) int64 {
	var totalAllocated int64

	podList, err := clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s,status.phase=Running", nodeName),
	})
	if err != nil {
		fmt.Printf("Error fetching pods for node %s: %v\n", nodeName, err)
		return totalAllocated
	}

	for _, pod := range podList.Items {
		for _, container := range pod.Spec.Containers {
			if value, ok := container.Resources.Requests[resourceName]; ok {
				allocated, _ := value.AsInt64()
				totalAllocated += allocated
			}
		}
	}
	return totalAllocated
}

func nodeAllocatableDiscovery(node *v1.Node) {
	// Get the node name from the environment variable set by the Downward API
	currentNodeName := os.Getenv("NODE_NAME")
	if currentNodeName == "" {
		fmt.Println("NODE_NAME environment variable not set")
		os.Exit(1)
	}

	// Use the in-cluster config. This will fall back to using the service account that is mounted by default by Kubernetes
	// But you need to be aware that this might not work if you are running this outside of Kubernetes
	config, err := rest.InClusterConfig()
	if err != nil {
		fmt.Printf("Error obtaining in-cluster config: %v\n", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error creating Kubernetes client: %v\n", err)
		os.Exit(1)
	}

	// Fetch only the details for the current node
	knode, err := clientset.CoreV1().Nodes().Get(context.TODO(), currentNodeName, metav1.GetOptions{})
	if err != nil {
		fmt.Printf("Error fetching node %s: %v\n", currentNodeName, err)
		os.Exit(1)
	}

	// Extract the allocatable and allocated resources from the current node
	gpuTotal := knode.Status.Capacity[corev1.ResourceName(gpuResourceName)]
	cpuTotal := knode.Status.Capacity[corev1.ResourceName(cpuResourceName)]
	memTotal := knode.Status.Capacity[corev1.ResourceName(memResourceName)]
	storageTotal := knode.Status.Capacity[corev1.ResourceName(storageResourceName)]

	AllocatedGPUs := getAllocatedResourceForNode(clientset, knode.Name, corev1.ResourceName(gpuResourceName))
	allocatedCPUs := getAllocatedResourceForNode(clientset, knode.Name, corev1.ResourceName(cpuResourceName))
	allocatedMemory := getAllocatedResourceForNode(clientset, knode.Name, corev1.ResourceName(memResourceName))
	allocatedStorage := getAllocatedResourceForNode(clientset, knode.Name, corev1.ResourceName(storageResourceName))

	AllocatedGPUsQuantity := resource.NewQuantity(int64(AllocatedGPUs), resource.DecimalSI)
	allocatedCPUsQuantity := resource.NewQuantity(int64(allocatedCPUs), resource.DecimalSI)
	allocatedMemoryQuantity := resource.NewQuantity(int64(allocatedMemory), resource.DecimalSI)
	allocatedStorageQuantity := resource.NewQuantity(int64(allocatedStorage), resource.DecimalSI)

	// Construct the GPU info
	var gpuInfos []*v1.GPUInfo
	if len(node.Gpus.Info) > 0 {
		gpuInfos = make([]*v1.GPUInfo, len(node.Gpus.Info))
		for i, info := range node.Gpus.Info {
			gpuInfos[i] = &v1.GPUInfo{
				Vendor:    info.Vendor,
				Name:      info.Name,
				ModelID:   info.ModelID,
				Interface: info.Interface,
				Memory:    info.Memory,
			}
		}
	}

	gpus := v1.GPU{
		Quantity: &v1.ResourcePair{
			Allocatable: gpuTotal,
			Allocated:   *AllocatedGPUsQuantity,
		},
		Info: gpuInfos,
	}

	// Construct the CPU info
	var cpuInfos []*v1.CPUInfo
	if len(node.CPU.Info) > 0 {
		cpuInfos = make([]*v1.CPUInfo, len(node.CPU.Info))
		for i, info := range node.CPU.Info {
			cpuInfos[i] = &v1.CPUInfo{
				ID:     info.ID,
				Vendor: info.Vendor,
				Model:  info.Model,
				Vcores: info.Vcores,
			}
		}
	}

	cpus := v1.CPU{
		Quantity: &v1.ResourcePair{
			Allocatable: cpuTotal,
			Allocated:   *allocatedCPUsQuantity,
		},
		Info: cpuInfos,
	}

	// Construct the Memory info
	memory := v1.Memory{
		Quantity: &v1.ResourcePair{
			Allocatable: memTotal,
			Allocated:   *allocatedMemoryQuantity,
		},
	}

	// Construct the Storage info
	storage := v1.Storage{
		Quantity: &v1.ResourcePair{
			Allocatable: storageTotal,
			Allocated:   *allocatedStorageQuantity,
		},
	}

	// Now the node structure is safely updated with the new information.
	node.Gpus = gpus
	node.CPU = cpus
	node.Memory = memory
	node.Storage = storage
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	v1.RegisterMsgServer(grpcServer, &msgServiceServer{})
	reflection.Register(grpcServer)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
