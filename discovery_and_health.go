package main

import (
	_ "consul-demo/consul"
	"context"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/k0kubun/pp/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type ServiceNameType string

func GetServiceInstanceID(ip string, port int, serviceName ServiceNameType) string {
	return fmt.Sprintf("%s:%d[%v]", ip, port, serviceName) // 服务实例ID
}

// consulClient 定义一个consul结构体，其内部有一个`*api.Client`字段。
type consulClient struct {
	client *api.Client
}

// NewConsulClient 连接至consul服务返回一个consul对象
func NewConsulClient(addr string) *consulClient {
	cfg := api.DefaultConfig()
	cfg.Address = addr
	c, err := api.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	return &consulClient{c}
}

// RegisterService 将grpc服务注册到consul
func (c *consulClient) RegisterService(ip string, port int, serviceName ServiceNameType) error {
	// 健康检查
	healthCheckConfig := &api.AgentServiceCheck{

		// 检查方式支持 HTTP GRPC 等, 详见结构体字段
		// https://learnku.com/articles/73175
		// https://studygolang.com/articles/32456
		// https://www.jianshu.com/p/fc4315e1c49d
		// https://github.com/grpc/grpc/blob/master/doc/health-checking.md

		// 检查策略多样, 有主动检查也有被动检查
		// https://developer.hashicorp.com/consul/tutorials/developer-discovery/service-registration-health-checks
		// https://developer.hashicorp.com/consul/api-docs/agent/check#json-request-body-schema
		// https://developer.hashicorp.com/consul/docs/services/usage/checks#grpc-check-configuration

		// 这里一定要指定可以被consul访问到的地址. 可以指定被检查的对象是整个 grpc server 还是 grpc server 上的某个服务.
		GRPC: fmt.Sprintf("%s:%d", ip, port),

		// Timeout (duration: 10s)
		// - Specifies a timeout for outgoing connections in the case of a Script, HTTP, TCP, UDP, or gRPC check.
		Timeout: "10s",

		// 运行检查的频率
		// Interval (string: "")
		// - Specifies the frequency at which to run this check. This is required for HTTP, TCP, and UDP checks.
		Interval: "10s",

		// 指定时间后consul自动剔除被认为不健康的条目
		// DeregisterCriticalServiceAfter (string: "")
		// - Specifies that checks associated with a service should deregister after this time.
		// This is specified as a time duration with suffix like "10m".
		// If a check is in the critical state for more than this configured value,
		// then its associated service (and all of its associated checks) will automatically be deregistered.
		// The minimum timeout is 1 minute, and the process that reaps critical services runs every 30 seconds,
		// so it may take slightly longer than the configured timeout to trigger the deregistration.
		// This should generally be configured with a timeout that's much, much longer than any expected recoverable outage for the given service.
		DeregisterCriticalServiceAfter: "1m",
	}
	srv := &api.AgentServiceRegistration{
		ID:      GetServiceInstanceID(ip, port, serviceName), // 服务实例ID
		Name:    string(serviceName),                         // 服务名称
		Tags:    []string{"tagFoo", "tagBar"},                // 为服务打标签
		Address: ip,
		Port:    port,
		Check:   healthCheckConfig,
	}
	return c.client.Agent().ServiceRegister(srv)
}

// DeregisterService 在注册表中移除grpc服务
func (c *consulClient) DeregisterService(serviceInstanceID string) error {
	return c.client.Agent().ServiceDeregister(serviceInstanceID)
}

func QueryServiceAndCallMethodWay1(serviceName ServiceNameType) {
	// 连接consul
	cc, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		fmt.Printf("api.NewClient failed, err:%v\n", err)
		return
	}
	// 返回的是一个 map[string]*api.AgentService
	// 其中key是服务ID，值是注册的服务信息
	serviceMap, err := cc.Agent().ServicesWithFilter(fmt.Sprintf("Service==`%v`", serviceName))
	if err != nil {
		fmt.Printf("query service from consulClient failed, err:%v\n", err)
		return
	}
	// 选一个服务机（这里选最后一个）
	var addr string
	for k, v := range serviceMap {
		pp.Println("k", k)
		pp.Println("v", v)
		addr = fmt.Sprintf("%s:%d", v.Address, v.Port)
	}

	// 对目标地址发起请求
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc.Dial failed,err:%v", err)
		return
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)
	resp, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: "xiaoqing way1 1st call"})
	if err != nil {
		fmt.Printf("c.SayHello failed, err:%v\n", err)
		return
	}
	fmt.Printf("resp:%v\n", resp.Message)
	resp, err = c.SayHello(context.Background(), &pb.HelloRequest{Name: "xiaoqing way1 2ed call"})
	if err != nil {
		fmt.Printf("c.SayHello failed, err:%v\n", err)
		return
	}
	fmt.Printf("resp:%v\n", resp.Message)
}

func QueryServiceAndCallMethodWay2(serviceName ServiceNameType) {

	// ClientConn represents a virtual connection to a conceptual endpoint, to
	// perform RPCs.
	//
	// A ClientConn is free to have zero or more actual connections to the endpoint
	// based on configuration, load, etc. It is also free to determine which actual
	// endpoints to use and may change it every RPC, permitting client-side load
	// balancing.
	//
	// A ClientConn encapsulates a range of functionality including name
	// resolution, TCP connection establishment (with retries and backoff) and TLS
	// handshakes. It also handles errors on established connections by
	// re-resolving the name and reconnecting.

	conn, err := grpc.Dial(
		fmt.Sprintf("consul://127.0.0.1:8500/%v?healthy=true", serviceName),     // consul服务
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy": "round_robin"}`), // client-side load balance
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("grpc.Dial failed,err:%v", err)
		return
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)
	resp, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: "xiaoqing way2 1st call"})
	if err != nil {
		fmt.Printf("c.SayHello failed, err:%v\n", err)
		return
	}
	fmt.Printf("resp:%v\n", resp.Message)
	resp, err = c.SayHello(context.Background(), &pb.HelloRequest{Name: "xiaoqing way2 2ed call"})
	if err != nil {
		fmt.Printf("c.SayHello failed, err:%v\n", err)
		return
	}
	fmt.Printf("resp:%v\n", resp.Message)
	resp, err = c.SayHello(context.Background(), &pb.HelloRequest{Name: "xiaoqing way2 3rd call"})
	if err != nil {
		fmt.Printf("c.SayHello failed, err:%v\n", err)
		return
	}
	fmt.Printf("resp:%v\n", resp.Message)
	resp, err = c.SayHello(context.Background(), &pb.HelloRequest{Name: "xiaoqing way2 4th call"})
	if err != nil {
		fmt.Printf("c.SayHello failed, err:%v\n", err)
		return
	}
	fmt.Printf("resp:%v\n", resp.Message)
}

func CallMethodDirectly() {
	conn, err := grpc.Dial(
		"127.0.0.1:50052",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("grpc.Dial failed,err:%v", err)
		return
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)
	resp, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: "call directly"})
	if err != nil {
		fmt.Printf("c.SayHello failed, err:%v\n", err)
		return
	}
	fmt.Printf("resp:%v\n", resp.Message)
}

func Main() {
	var helloServiceName ServiceNameType = "hello"

	// 第一个服务实例
	serviceIp := "127.0.0.1"
	servicePort := 50051

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", servicePort))
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}
	s := grpc.NewServer()

	// 开启健康检查
	healthcheck := health.NewServer()
	healthpb.RegisterHealthServer(s, healthcheck)
	pb.RegisterGreeterServer(s, &server{})

	// 启动服务
	go func() {
		err = s.Serve(lis)
		if err != nil {
			log.Printf("failed to serve: %v", err)
			return
		}
	}()

	//第二个服务实例
	serviceIp2 := "127.0.0.1"
	servicePort2 := 50052

	lis2, err := net.Listen("tcp", fmt.Sprintf(":%d", servicePort2))
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}
	s2 := grpc.NewServer()

	// 开启健康检查
	healthcheck2 := health.NewServer()
	healthpb.RegisterHealthServer(s2, healthcheck2)
	pb.RegisterGreeterServer(s2, &server2{})

	// 启动服务
	go func() {
		err = s2.Serve(lis2)
		if err != nil {
			log.Printf("failed to serve: %v", err)
			return
		}
	}()

	time.Sleep(time.Second * 1) // let the servers become ready

	consul := NewConsulClient("127.0.0.1:8500")
	// 注册服务
	consul.RegisterService(serviceIp, servicePort, helloServiceName)
	consul.RegisterService(serviceIp2, servicePort2, helloServiceName)

	QueryServiceAndCallMethodWay1(helloServiceName)
	fmt.Println()
	QueryServiceAndCallMethodWay2(helloServiceName)
	fmt.Println()
	CallMethodDirectly()
	fmt.Println()

	pp.Println("wait util you exit...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	// 退出时注销服务
	consul.DeregisterService(GetServiceInstanceID(serviceIp, servicePort, helloServiceName))
	consul.DeregisterService(GetServiceInstanceID(serviceIp2, servicePort2, helloServiceName))
}

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "response form server 1:" + in.GetName()}, nil
}

// server is used to implement helloworld.GreeterServer.
type server2 struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer
func (s *server2) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "response form server 2 " + in.GetName()}, nil
}

func main() {

	//2023/05/12 17:39:08 Received: xiaoqing way1 1st call
	//resp:response form server 2 xiaoqing way1 1st call
	//2023/05/12 17:39:08 Received: xiaoqing way1 2ed call
	//resp:response form server 2 xiaoqing way1 2ed call
	//
	//2023/05/12 17:39:08 Received: xiaoqing way2 1st call
	//resp:response form server 1:xiaoqing way2 1st call
	//2023/05/12 17:39:08 Received: xiaoqing way2 2ed call
	//resp:response form server 2 xiaoqing way2 2ed call
	//2023/05/12 17:39:08 Received: xiaoqing way2 3rd call
	//resp:response form server 1:xiaoqing way2 3rd call
	//2023/05/12 17:39:08 Received: xiaoqing way2 4th call
	//resp:response form server 2 xiaoqing way2 4th call
	//
	//2023/05/12 17:39:08 Received: call directly
	//resp:response form server 2 call directly

	Main()
}
