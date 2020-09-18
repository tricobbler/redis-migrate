package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"strings"
)

var sourceAddr = flag.String("saddr", "", "Input source redis address")
var sourcePwd = flag.String("spwd", "", "Input source redis password")
var sourceDb = flag.Int("sdb", 0, "Input source redis db number")

var targetAddr = flag.String("taddr", "", "Input target redis address")
var targetPwd = flag.String("tpwd", "", "Input target redis password")
var targetDb = flag.Int("tdb", 1, "Input target redis db number")

func main() {
	flag.Parse()

	var sourceClient = GetRedisConn(*sourceAddr, *sourcePwd, *sourceDb)
	var targetClient = GetRedisConn(*targetAddr, *targetPwd, *targetDb)

	keyList := sourceClient.Keys("*").Val()

	for m := 0; m < len(keyList); {
		pType := sourceClient.Pipeline()

		if m+10 < len(keyList) {
			for n := 0; n < 10; n++ {
				pType.Type(keyList[m+n])
			}
		} else {
			max := len(keyList) - m
			for n := 0; n < max; n++ {
				pType.Type(keyList[n+m])
			}
		}
		typeResult, err := pType.Exec()

		pType.Close()

		if err != nil {
			fmt.Println("aaaaaaaaaaaaa")
		}

		pp := targetClient.Pipeline()

		for index := 0; index < len(typeResult); index++ {
			typeResultSplitArray := strings.Split(fmt.Sprintf("%s", typeResult[index]), ":")
			keyType := strings.Trim(typeResultSplitArray[len(typeResultSplitArray)-1], " ")

			switch keyType {
			case "string":
				{
					keyValue := targetClient.Get(keyList[m+index]).Val()
					pp.Set(keyList[m+index], keyValue, -1)
				}
			case "list":
				{
					//targetClient.l
				}
			case "set":
				{
					array, _ := sourceClient.SMembers(keyList[m+index]).Result()
					for _, v := range array {
						pp.SAdd(keyList[m+index], v)
					}
				}
			case "zset":
				{
				 zResult,_:= sourceClient.ZRangeWithScores(keyList[m+index],0,-1).Result()
				 for _,b:=range zResult{
				 	pp.ZAdd(keyList[m+index],redis.Z{Member: b.Member,Score: b.Score})
				 }
				}
			case "hash":
				{
					fields, _ := sourceClient.HGetAll(keyList[m+index]).Result()
					for s, f := range fields {
						pp.HSet(keyList[m+index], s, f)
					}
				}
			}
		}
		pp.Exec()
		pp.Close()

		m = m + 10
	}
}

//获取redis集群客户端
func GetRedisConn(addr, pwd string, db int) *redis.Client {
	var redisHandle *redis.Client

	redisHandle = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       db,
		//MinIdleConns: 256,
		//IdleTimeout:  30,
	})
	_, err := redisHandle.Ping().Result()
	if err != nil {
		panic(err)
	}

	return redisHandle
}
