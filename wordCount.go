package main

import (
	"./goMapReduce"
	"fmt"
	"strings"
	"sync"
)

type MyMapReduce struct {
	reduceCriticalValue int
}

func (mr *MyMapReduce) CustomMap(mapData interface{}, mapOutChannel chan goMapReduce.MRChanData) {
	str := mapData.(string)
	tokens := strings.Fields(str)

	for i := range tokens {

		word := strings.Trim(strings.ToLower(tokens[i]), ",_-.:/*!;-=() \"'[]{}+")
		if len(word) > 1 {
			mapOutChannel <- goMapReduce.MRChanData{word, 1}
		}

	}
}

func (mr *MyMapReduce) CustomReduce(key string, values []interface{}, reduceOutChannel chan goMapReduce.MRChanData) {
	reduceOutChannel <- goMapReduce.MRChanData{key, len(values)}
}

func writeInTaskData(mapInChannel chan goMapReduce.MRChanData) {
	taskData := map[string]string{
		"task1": "primer trozo de informacion para procesado primer trozo",
		"task2": "segundo trozo de informacion trozo de",
		"task3": "otro trozo para ser procesado otro otro otro trozo",
		"task4": "primer trozo de informacion para procesado primer trozo",
		"task5": "segundo trozo de informacion trozo de",
		"task6": "otro trozo para ser procesado otro otro otro trozo",
	}

	for k := range taskData {
		mapInChannel <- goMapReduce.MRChanData{k, taskData[k]}
	}

	close(mapInChannel)
}

func handleResult(kv goMapReduce.MRChanData) {
	fmt.Printf("[%s, %d], word: %s appear %d times.\n", kv.Key, kv.Value.(int), kv.Key, kv.Value.(int))
}

func main() {
	myMapReduce := MyMapReduce{reduceCriticalValue: 5000}

	gmr := goMapReduce.NewMapReduce(&myMapReduce, myMapReduce.reduceCriticalValue)

	mapInChannel := gmr.GetMapInChannel()

	reduceOutChannel := gmr.GetreduceOutChannel()

	gmr.Run()

	//write task in channel
	go writeInTaskData(mapInChannel)
	//read result from channel
	var resultWaitGroup sync.WaitGroup
	for kv := range reduceOutChannel {
		//run handleResult function concurrency
		go func(kvPair goMapReduce.MRChanData) {
			resultWaitGroup.Add(1)
			handleResult(kvPair)
			resultWaitGroup.Done()
		}(kv)
	}
	//when all result has been handled, exit
	resultWaitGroup.Wait()
	fmt.Println("Count Word program done!")
}
