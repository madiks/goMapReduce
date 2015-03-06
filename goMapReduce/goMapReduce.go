package goMapReduce

import (
	"sync"
)

type MRChanData struct {
	Key   string
	Value interface{}
}

type UserCustomMapReduce interface {
	CustomMap(mapData interface{}, mapOutChannel chan MRChanData)

	CustomReduce(key string, values []interface{}, reduceOutChannel chan MRChanData)
}

type MapReduce struct {
	mapInChannel     chan MRChanData
	mapOutChannel    chan MRChanData
	reduceInChannel  chan MRChanData
	reduceOutChannel chan MRChanData
	customMapReduce  *UserCustomMapReduce

	aggregate map[string][]interface{}
	//reduce start Critical Value
	reduceCriticalValue int
}

func NewMapReduce(uc UserCustomMapReduce, reduceCriticalValue int) *MapReduce {
	mapReduce := &MapReduce{
		mapInChannel:        make(chan MRChanData),
		mapOutChannel:       make(chan MRChanData),
		reduceInChannel:     make(chan MRChanData),
		reduceOutChannel:    make(chan MRChanData),
		customMapReduce:     &uc,
		aggregate:           make(map[string][]interface{}),
		reduceCriticalValue: reduceCriticalValue,
	}
	return mapReduce
}

func (mapReduce *MapReduce) GetMapInChannel() (mapInChannel chan MRChanData) {
	return mapReduce.mapInChannel
}

func (mapReduce *MapReduce) GetreduceOutChannel() (mapInChannel chan MRChanData) {
	return mapReduce.reduceOutChannel
}

func (mapReduce *MapReduce) ListenMapIn() {
	var mapWaitGroup sync.WaitGroup
	for mapInData := range mapReduce.mapInChannel {
		go func(mapData interface{}, mapOutChannel chan MRChanData) {
			mapWaitGroup.Add(1)
			(*mapReduce.customMapReduce).CustomMap(mapData, mapOutChannel)
			mapWaitGroup.Done()
		}(mapInData.Value, mapReduce.mapOutChannel)
	}
	//when all mapper goroutine done close mapOut channel
	mapWaitGroup.Wait()
	close(mapReduce.mapOutChannel)
}

func (mapReduce *MapReduce) aggregateMapOut() {
	for mapOutData := range mapReduce.mapOutChannel {
		if len(mapReduce.aggregate[mapOutData.Key]) == mapReduce.reduceCriticalValue {
			mapReduce.reduceInChannel <- MRChanData{mapOutData.Key, mapReduce.aggregate[mapOutData.Key]}
			mapReduce.aggregate[mapOutData.Key] = mapReduce.aggregate[mapOutData.Key][:0]
		}
		mapReduce.aggregate[mapOutData.Key] = append(mapReduce.aggregate[mapOutData.Key], mapOutData.Value)
	}

	for k := range mapReduce.aggregate {
		mapReduce.reduceInChannel <- MRChanData{k, mapReduce.aggregate[k]}
	}

	close(mapReduce.reduceInChannel)
}

func (mapReduce *MapReduce) ListenReduceIn() {
	var reduceWaitGroup sync.WaitGroup
	for reduceInData := range mapReduce.reduceInChannel {
		go func(key string, values []interface{}, reduceOutChannel chan MRChanData) {
			reduceWaitGroup.Add(1)
			(*mapReduce.customMapReduce).CustomReduce(key, values, reduceOutChannel)
			reduceWaitGroup.Done()
		}(reduceInData.Key, reduceInData.Value.([]interface{}), mapReduce.reduceOutChannel)
	}
	//when all reducer goroutine done close reduceOut channel
	reduceWaitGroup.Wait()
	close(mapReduce.reduceOutChannel)
}

func (mapReduce *MapReduce) Run() {

	go mapReduce.ListenMapIn()

	go mapReduce.aggregateMapOut()

	go mapReduce.ListenReduceIn()

}
