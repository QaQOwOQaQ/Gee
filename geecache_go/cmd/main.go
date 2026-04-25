package main

import (
	"flag"
	"fmt"
	"geecache"
	"log"
	"net/http"
	"time"
)

// 模拟数据库
var db = map[string]string{
	"Tom":   "630",
	"Jack":  "589",
	"Sam":   "567",
	"Alice": "999",
}

// 集群中所有节点地址
var peers = []string{
	"http://localhost:8001",
	"http://localhost:8002",
	"http://localhost:8003",
}

// createGroup 创建缓存组，缓存未命中时从模拟数据库加载，TTL 设为 1 分钟
func createGroup() *geecache.Group {
	return geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Printf("[模拟数据库] 查询 key=%s", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("key %s 在数据库中不存在", key)
		},
	), 60*time.Second, 10*time.Second)
}

// startServer 以 server 模式运行：启动当前节点的 HTTP 缓存服务
//
// 真实部署时，每台机器运行一个实例，通过 -port 区分自身地址。
// 用法：go run cmd/main.go -port 8001
func startServer(port int) {
	self := fmt.Sprintf("http://localhost:%d", port)

	group := createGroup()

	// 创建 HTTP 节点池，注册集群中所有节点
	pool := geecache.NewHTTPPool(self)
	pool.Set(peers...)
	group.RegisterPeers(pool) // 每个进程只调用一次

	log.Printf("[节点] %s 启动，集群节点: %v", self, peers)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), pool))
}

// runDemo 以 demo 模式运行：在单进程内演示缓存命中/未命中行为（无分布式）
//
// 用法：go run cmd/main.go -demo
func runDemo() {
	group := createGroup()

	fmt.Println("\n========== 演示开始 ==========")

	// 演示 1：第一次查询 → 缓存未命中 → 从数据库加载
	fmt.Println("\n[演示 1] 第一次查询 Tom（缓存未命中，触发数据库查询）")
	v, err := group.Get("Tom")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("结果: Tom 的分数 = %s\n", v)

	// 演示 2：第二次查询同一 key → 缓存命中 → 不查数据库
	fmt.Println("\n[演示 2] 第二次查询 Tom（应命中缓存，不再查询数据库）")
	v, err = group.Get("Tom")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("结果: Tom 的分数 = %s\n", v)

	// 演示 3：查询多个不同 key，相同 key 第二次命中缓存
	fmt.Println("\n[演示 3] 查询多个 key（Jack 查询两次，第二次命中缓存）")
	for _, key := range []string{"Jack", "Sam", "Alice", "Jack"} {
		v, err := group.Get(key)
		if err != nil {
			log.Printf("查询 %s 失败: %v", key, err)
			continue
		}
		fmt.Printf("结果: %s 的分数 = %s\n", key, v)
	}

	// 演示 4：查询不存在的 key
	fmt.Println("\n[演示 4] 查询不存在的 key \"Unknown\"")
	_, err = group.Get("Unknown")
	if err != nil {
		fmt.Printf("预期错误: %v\n", err)
	}

	// 演示 5：Delete 写后失效（Cache Aside 写路径）
	fmt.Println("\n[演示 5] 更新数据库后调用 Delete 使缓存失效")
	db["Tom"] = "888"
	if err := group.Delete("Tom"); err != nil {
		fmt.Printf("Delete 失败: %v\n", err)
	}
	v, err = group.Get("Tom")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("结果: Tom 的新分数 = %s（应为 888）\n", v)

	fmt.Println("\n========== 演示结束 ==========")
	fmt.Println("提示：'[模拟数据库]' 只在缓存未命中时出现，相同 key 第二次查询不会触发")
	fmt.Println("注意：Delete 会先删本地缓存；若 key owner 在远端，还会请求 owner 失效，失败返回 error")
}

func main() {
	port := flag.Int("port", 0, "以 server 模式启动节点，指定监听端口（如 8001）")
	demo := flag.Bool("demo", false, "以 demo 模式运行，演示缓存命中/未命中行为")
	flag.Parse()

	switch {
	case *demo:
		runDemo()
	case *port != 0:
		startServer(*port)
	default:
		fmt.Println("用法:")
		fmt.Println("  单进程演示:     go run cmd/main.go -demo")
		fmt.Println("  启动节点 8001:  go run cmd/main.go -port 8001")
		fmt.Println("  启动节点 8002:  go run cmd/main.go -port 8002")
		fmt.Println("  启动节点 8003:  go run cmd/main.go -port 8003")
	}
}
