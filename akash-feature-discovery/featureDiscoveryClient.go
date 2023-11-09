package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	v1 "akash-api/v1"

	"google.golang.org/grpc"
)

const (
	kubeConfigPath         = "./current.kubeconfig"
	daemonSetLabelSelector = "app=hostfeaturediscovery"
	daemonSetNamespace     = "akash-services"
	grpcPort               = ":50051"
	nodeUpdateInterval     = 5 * time.Second // Duration after which to print the cluster state
	Added                  = "ADDED"
	Deleted                = "DELETED"
)

// ReceivedNodeData holds the node data along with the last received timestamp.
type ReceivedNodeData struct {
	Node  *v1.Node
	Stamp time.Time
}

// Global node data map and its mutex
var (
	nodeDataMap  = make(map[string]ReceivedNodeData)
	nodeDataLock = sync.Mutex{}
)

func connectToGrpcStream(pod *corev1.Pod) {
	ipAddress := fmt.Sprintf("%s%s", pod.Status.PodIP, grpcPort)
	fmt.Println("Connecting to:", ipAddress)
	conn, err := grpc.Dial(ipAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("did not connect to pod IP %s: %v", pod.Status.PodIP, err)
		return
	}
	defer conn.Close()

	client := v1.NewMsgClient(conn)

	// Using context.Background() for an indefinite timeout. The stream will remain open
	// indefinitely until an error occurs or the server closes the stream.
	stream, err := client.QueryNode(context.Background(), &v1.VoidNoParam{})
	if err != nil {
		log.Printf("could not query node for pod IP %s: %v", pod.Status.PodIP, err)
		return
	}

	for {
		node, err := stream.Recv()
		if err != nil {
			// Stream closed, remove by UID
			nodeDataLock.Lock()
			delete(nodeDataMap, string(pod.UID))
			nodeDataLock.Unlock()
			log.Printf("Stream closed for pod UID %s: %v", pod.UID, err)
			break
		}

		nodeDataLock.Lock()
		nodeDataMap[string(pod.UID)] = ReceivedNodeData{Node: node, Stamp: time.Now()}
		nodeDataLock.Unlock()
	}
}

func printCluster() {
	nodeDataLock.Lock()
	defer nodeDataLock.Unlock()

	// If no nodes to print, just return
	if len(nodeDataMap) == 0 {
		return
	}

	cluster := &v1.Cluster{
		Nodes: make([]v1.Node, 0, len(nodeDataMap)),
	}

	for _, data := range nodeDataMap {
		// Make a deep copy of the node to avoid race conditions
		nodeCopy := *data.Node
		cluster.Nodes = append(cluster.Nodes, nodeCopy)
	}

	fmt.Printf("Cluster State: %+v\n\n\n", cluster)
}

func watchPods(clientset *kubernetes.Clientset, stopCh <-chan struct{}) {
	watcher, err := clientset.CoreV1().Pods(daemonSetNamespace).Watch(context.TODO(), metav1.ListOptions{
		LabelSelector: daemonSetLabelSelector,
	})
	if err != nil {
		log.Fatalf("Error setting up Kubernetes watcher: %v", err)
	}
	defer watcher.Stop()

	for {
		select {
		case <-stopCh:
			log.Println("Stopping pod watcher")
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				log.Println("Watcher channel closed")
				return
			}

			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				log.Println("Unexpected type in watcher event")
				continue
			}

			switch event.Type {
			case Added:
				if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
					// Pod is running and has an IP, so we can attempt to connect
					go connectToGrpcStream(pod)
				} else {
					// Pod is not yet ready, let's watch its status in a separate goroutine
					go waitForPodReadyAndConnect(clientset, pod)
				}
			case Deleted:
				nodeDataLock.Lock()
				delete(nodeDataMap, string(pod.UID))
				nodeDataLock.Unlock()
				log.Printf("Pod deleted: %s, UID: %s\n", pod.Name, pod.UID)
			}
		}
	}
}

// waitForPodReadyAndConnect waits for a pod to become ready before attempting to connect to its gRPC stream
func waitForPodReadyAndConnect(clientset *kubernetes.Clientset, pod *corev1.Pod) {
	for {
		// Polling the pod's status periodically
		time.Sleep(2 * time.Second)
		currentPod, err := clientset.CoreV1().Pods(pod.Namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
		if err != nil {
			log.Printf("Error getting pod status: %v\n", err)
			continue
		}

		if currentPod.Status.Phase == corev1.PodRunning && currentPod.Status.PodIP != "" {
			// Pod is now running and has an IP, we can connect to it
			connectToGrpcStream(currentPod)
			return
		}
	}
}

func main() {
	fmt.Println("Starting up gRPC client...")

	config, err := rest.InClusterConfig()
	if err != nil {
		config, err = clientcmd.BuildConfigFromFlags("", kubeConfigPath)
		if err != nil {
			log.Fatalf("Error building kubeconfig: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating Kubernetes client: %v", err)
	}

	// Start the watcher in a goroutine
	stopCh := make(chan struct{})
	defer close(stopCh)
	go watchPods(clientset, stopCh)

	// Start a ticker to print the cluster state every nodeUpdateInterval seconds
	ticker := time.NewTicker(nodeUpdateInterval)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			printCluster()
		}
	}()

	// Block main goroutine to keep it running
	select {}
}
