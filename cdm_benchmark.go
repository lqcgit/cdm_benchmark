/*
* @Date: 2024-06-20
* @Author: Kaka
* @Description:cdm_server的性能测试工具。为确保请求生成速度不成为瓶颈，工具优先根据测试请求，生成请求数据完成后，再并发
* @LastEditors: Kaka
* @LastEditTime: 2024-07-7 14:31:41
* @version 1.1
*@Copyright: Copyright(C) 2000-2024 达梦数据技术（江苏）有限公司
 */
package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup
var file *os.File
var logger *log.Logger

//
//var k map[int]map[int]string
//var v map[int]map[int]string
var k, f, m, zm sync.Map

//
var address = flag.String("address", "127.0.0.1:6379", "测试数据库访问地址，多个proxy时候，以','隔开")
var db = flag.Int("db", 0, "测试的db")
var cmd = flag.String("cmd", "set", "测试命令")
var conns = flag.Int("c", 20, "客户端数量")
var number = flag.Int("n", 100, "每个连接发起的请求总量")
var items = flag.Int("i", 16, "容器类命令元素个数,批量写入命令表示批量写入的条数")
var pwd = flag.String("pwd", "", "cdm连接密码")
var clean = flag.String("clean", "no", "是否开启测试后数据清理，yes表示开启测试后清理数据")
var output = flag.String("output", "yes", "是否开启控制台打印,yes表示打开控制台打印")
var vl = flag.Int("vl", 10, "value的长度")
var kl = flag.Int("kl", 10, "key的长度")
var addTime = flag.Bool("addTime", false, "多proxy场景key添加12位时间戳后缀")
var clusterMode = flag.Bool("clusterMode", false, "多proxy场景key添加12位时间戳后缀")

func main() {
	maxProcs := runtime.NumCPU() //获取cpu个数
	runtime.GOMAXPROCS(maxProcs)
	flag.Usage = func() {
		flag.PrintDefaults()
	}
	flag.Parse()
	fmt.Println("Start testing with cdm_benchmark V1.0\n")
	initLogger()
	*cmd = strings.ToUpper(*cmd)
	now := time.Now().UnixNano()
	value := RandStringRunes(*vl)
	addr := strings.Split(*address, ",")
	logger.Println("开始生成测试数据...\n")
	if *output == "yes" {
		fmt.Println("开始生成测试数据...\n")
	}
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func(i int) {
			PrepareData(i)
			wg.Done()
		}(i)
	}
	wg.Wait()
	t := fmt.Sprintf("测试数据准备时长为 %.3fms\n", float64(time.Now().UnixNano()-now)/float64(1e6))
	logger.Printf(time.Now().String() + "--" + t)
	logger.Printf(time.Now().String() + "--" + "并发测试开始\n")
	if *output == "yes" {
		fmt.Printf(time.Now().String() + "--" + t)
		fmt.Printf(time.Now().String() + "--" + "并发测试开始\n")
	}
	start_time := time.Now().UnixNano()
	//redis 连接
	Conns := make([]*redis.Client, *conns)
	CConns := make([]*redis.ClusterClient, *conns)
	if *clusterMode {
		for i := 0; i < *conns; i++ {
			CConns[i] = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:        addr,
				DialTimeout:  10 * time.Second,
				ReadTimeout:  300 * time.Second,
				WriteTimeout: 300 * time.Second,
				Password:     *pwd,
			})

			_, err := Conns[i].Ping().Result()
			if err != nil {
				panic(err)
			}
			wg.Add(1)
			go func(i int) {
				switch *cmd {
				case "SET":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					CsetTest(key, value, CConns[i])
				case "GET":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					CgetTest(key, CConns[i])
				case "HSET":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					ChsetTest(key, CConns[i])
				case "HGET":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)

					ChgetTest(key, CConns[i])

				case "LPUSH":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					members1, _ := m.Load(i)
					members := members1.(sync.Map)
					ClpushTest(key, members, CConns[i])
				case "LRANGE":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					ClrangeTest(key, CConns[i])
				case "SADD":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					members1, _ := m.Load(i)
					members := members1.(sync.Map)
					CsaddTest(key, members, CConns[i])
				case "SMEMBERS":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					CsmembersTest(key, CConns[i])
				case "ZADD":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					zm1, _ := zm.Load(i)
					zmember := zm1.(sync.Map)
					CzaddTest(key, zmember, CConns[i])
				case "ZRANGE":
					key1, _ := k.Load(i)
					key := key1.(sync.Map)
					CzrangeTest(key, CConns[i])
				}

				defer wg.Done()
			}(i)

			defer Conns[i].Close()
		}
	} else { //有多个proxy，就按照多个proxy的总数
		for j := 0; j < len(addr); j++ {
			for i := 0; i < *conns; i++ {
				Conns[i] = redis.NewClient(&redis.Options{
					Addr:         addr[j],
					DialTimeout:  10 * time.Second,
					ReadTimeout:  300 * time.Second,
					WriteTimeout: 300 * time.Second,
					Password:     *pwd,
					DB:           *db,
				})
				_, err := Conns[i].Ping().Result()
				if err != nil {
					panic(err)
				}
				wg.Add(1)
				go func(i int) {
					switch *cmd {
					case "SET":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						setTest(key, value, Conns[i])
					case "GET":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						getTest(key, Conns[i])
					case "HSET":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						hsetTest(key, Conns[i])
					case "HGET":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)

						hgetTest(key, Conns[i])

					case "LPUSH":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						members1, _ := m.Load(i)
						members := members1.(sync.Map)
						lpushTest(key, members, Conns[i])

					case "LRANGE":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						lrangeTest(key, Conns[i])
					case "SADD":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						members1, _ := m.Load(i)
						members := members1.(sync.Map)
						saddTest(key, members, Conns[i])
					case "SMEMBERS":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						smembersTest(key, Conns[i])
					case "ZADD":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						zm1, _ := zm.Load(i)
						zmember := zm1.(sync.Map)
						zaddTest(key, zmember, Conns[i])
					case "ZRANGE":
						key1, _ := k.Load(i)
						key := key1.(sync.Map)
						zrangeTest(key, Conns[i])
					}
					defer wg.Done()
				}(i)

				defer Conns[i].Close()
			}

		}
	}
	wg.Wait()
	dutation := float64(time.Now().UnixNano()-start_time) / float64(1e6)
	//fmt.Printf("proxy数量：%d \n", len(addr))
	//fmt.Printf("请求数：%.3f \n",float64(*mm**number**conns*len(addr)))
	//fmt.Printf("单个请求处理的平均时长：%.3f ms\n",dutation/float64(*mm**number**conns*len(addr)))
	var txt string
	if *clusterMode {
		txt = fmt.Sprintf("测试的命令为：%s，客户端数量:%d,每个连接发起请求数量为：%d,总请求数量为：%d,容器类命令每个key的元素个数为：%d，执行时长为: %.3fms,QPS为%.3f,单请求处理平均耗时为：%.6fms \n",
			*cmd, *conns, *number, *number**conns, *items, dutation, float64(*number**conns)*1000/dutation, dutation/float64(*number**conns))
	} else {
		txt = fmt.Sprintf("测试的命令为：%s，客户端数量:%d,每个连接发起请求数量为：%d,总请求数量为：%d,容器类命令每个key的元素个数为：%d，执行时长为: %.3fms,QPS为%.3f,单请求处理平均耗时为：%.6fms \n",
			*cmd, *conns, *number, *number**conns*len(addr), *items, dutation, float64(*number**conns*len(addr))*1000/dutation, dutation/float64(*number**conns*len(addr)))
	}
	logger.Printf(time.Now().String() + "--" + txt)

	if *output == "yes" {
		fmt.Printf(time.Now().String() + "--" + txt)
	}

	if *clean == "yes" {
		clean_c := redis.NewClient(&redis.Options{
			Addr:         *address,
			DialTimeout:  10 * time.Second,
			ReadTimeout:  300 * time.Second,
			WriteTimeout: 300 * time.Second,
			Password:     *pwd,
			DB:           *db,
		})
		clean_c.FlushDB()
		clean_c.Close()
	}
}

func setTest(key sync.Map, v string, conn *redis.Client) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		//fmt.Printf(key + "\n")
		err := conn.Set(key, v, 0).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func CsetTest(key sync.Map, v string, conn *redis.ClusterClient) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		//fmt.Printf(key + "\n")
		err := conn.Set(key, v, 0).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}
func getTest(key sync.Map, conn *redis.Client) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		result, err := conn.Get(key).Result()
		if err != nil && result != "" {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}
func CgetTest(key sync.Map, conn *redis.ClusterClient) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		result, err := conn.Get(key).Result()
		if err != nil && result != "" {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func hsetTest(key sync.Map, conn *redis.Client) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		err := conn.HSet(key, "filed_test", 0).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func ChsetTest(key sync.Map, conn *redis.ClusterClient) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		err := conn.HSet(key, "filed_test", 0).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func hgetTest(key sync.Map, conn *redis.Client) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		err := conn.HGet(key, "filed_test").Err()
		//result, err := conn.HGet(key, "filed_test").Result()
		//fmt.Printf(result)
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func ChgetTest(key sync.Map, conn *redis.ClusterClient) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		err := conn.HGet(key, "filed_test").Err()
		//result, err := conn.HGet(key, "filed_test").Result()
		//fmt.Printf(result)
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func lpushTest(key sync.Map, member sync.Map, conn *redis.Client) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		member, _ := member.Load(j)
		err := conn.LPush(key, member).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func ClpushTest(key sync.Map, member sync.Map, conn *redis.ClusterClient) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		member, _ := member.Load(j)
		err := conn.LPush(key, member).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func lrangeTest(key sync.Map, conn *redis.Client) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		err := conn.LRange(key, 0, -1).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func ClrangeTest(key sync.Map, conn *redis.ClusterClient) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		err := conn.LRange(key, 0, -1).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func saddTest(key sync.Map, member sync.Map, conn *redis.Client) {
	var member_real interface{}
	for j := 0; j < *items; j++ {
		member_real, _ = member.Load(j)
	}
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		err := conn.SAdd(key, member_real).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func CsaddTest(key sync.Map, member sync.Map, conn *redis.ClusterClient) {
	var member_real interface{}
	for j := 0; j < *items; j++ {
		member_real, _ = member.Load(j)
	}
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		err := conn.SAdd(key, member_real).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func smembersTest(key sync.Map, conn *redis.Client) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		err := conn.SMembers(key).Err()
		//result, err := conn.SMembers(key).Result()
		//fmt.Println(result)
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}
func CsmembersTest(key sync.Map, conn *redis.ClusterClient) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		err := conn.SMembers(key).Err()
		//result, err := conn.SMembers(key).Result()
		//fmt.Println(result)
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func zaddTest(key sync.Map, zmembers sync.Map, conn *redis.Client) {
	zmember := make([]redis.Z, *items)
	for j := 0; j < *items; j++ {
		zm1, _ := zmembers.Load(j)
		zmember, _ = zm1.([]redis.Z)
	}
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		err := conn.ZAdd(key, zmember...).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func CzaddTest(key sync.Map, zmembers sync.Map, conn *redis.ClusterClient) {
	zmember := make([]redis.Z, *items)
	for j := 0; j < *items; j++ {
		zm1, _ := zmembers.Load(j)
		zmember, _ = zm1.([]redis.Z)
	}
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		var key string
		if *addTime {
			key = k1.(string) + time.Now().String()[len(time.Now().String())-12:]
		} else {
			key = k1.(string)
		}
		err := conn.ZAdd(key, zmember...).Err()
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func zrangeTest(key sync.Map, conn *redis.Client) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		err := conn.ZRange(key, 0, -1).Err()
		//result, err := conn.ZRange(key, 0, -1).Result()
		//fmt.Println(result)
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func CzrangeTest(key sync.Map, conn *redis.ClusterClient) {
	for j := 0; j < *number; j++ {
		k1, _ := key.Load(j)
		key := k1.(string)
		err := conn.ZRange(key, 0, -1).Err()
		//result, err := conn.ZRange(key, 0, -1).Result()
		//fmt.Println(result)
		if err != nil {
			logger.Printf(time.Now().String() + "--" + "命令执行失败，详细信息如下：" + err.Error())
		}
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

//随机生成指定长度字符串
func RandStringRunes(n int) string {
	b := make([]rune, n)
	rand.Seed(time.Now().UnixNano())
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

//根据测试命令，准备测试请求需要的数据
func PrepareData(i int) {
	//根据测试输入参数每个连接的请求数量number和key的长度kl，生成对应长度的key
	var s sync.Map
	var k1 string
	for j := 0; j < *number; j++ {
		//开启生成规则的key值，默认生成key的长度为10
		if *kl >= 9 {
			k1 = *cmd + fmt.Sprintf("%03d", i) + RandStringRunes(*kl-9) + fmt.Sprintf("%06d", j)
		} else {
			if *kl >= 3 {
				k1 = *cmd + RandStringRunes(*kl-3) + fmt.Sprintf("%03d", i)
			} else {
				k1 = *cmd + fmt.Sprintf("%5d", j)
			}
		}
		s.Store(j, k1)
	}
	k.Store(i, s)
	//hash类型命令需要生成filed
	if *cmd == "HMSET" {
		var sf sync.Map
		var hk string
		for j := 0; j < *items; j++ {
			rand.Seed(time.Now().UnixNano())
			if *kl >= 3 {
				hk = RandStringRunes(*kl-3) + fmt.Sprintf("%03d", i)
			} else {
				hk = fmt.Sprintf("%5d", j)
			}

			sf.Store(j, hk)
		}
		f.Store(i, sf)
	}
	//set类型和list类型，生成指定个数的members
	if *cmd == "SADD" || *cmd == "LPUSH" || *cmd == "RPUSH" {
		var sm sync.Map
		member := make([]string, *items)
		for j := 0; j < *items; j++ {
			if *kl >= 3 {
				member[j] = RandStringRunes(*kl-3) + fmt.Sprintf("%03d", j)
			} else {
				member[j] = fmt.Sprintf("%5d", j)
			}
			sm.Store(j, member)
		}
		m.Store(i, sm)
	}
	//为zadd生成指定个数的元素和对应的分数
	if *cmd == "ZADD" {
		var s3 sync.Map
		zmember := make([]redis.Z, *items)
		for j := 0; j < *items; j++ {
			rand.Seed(time.Now().UnixNano())
			if *kl >= 3 {
				zmember[j].Member = RandStringRunes(*vl-1) + strconv.Itoa(j)
			} else {
				zmember[j].Member = fmt.Sprintf("%5d", j)
			}
			zmember[j].Score = rand.Float64()
			s3.Store(j, zmember)
		}
		zm.Store(i, s3)
	}
}

func initLogger() {
	var err error
	file, err = os.OpenFile(fmt.Sprintf("cdm_benchmark%s.log", time.Now().Format("2006-01-02")), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	logger = log.New(file, "", log.LstdFlags)
	logger.SetFlags(0)
	//设置每一行的前缀
	logger.SetPrefix("********************\n")

}
