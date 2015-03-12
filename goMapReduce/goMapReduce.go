package goMapReduce

import (
	"sync"
)

type MRChanData struct {
	Key   string
	Value interface{}
}

type userCustomMapReduce interface {
	CustomMap(mapData interface{}, mapOutChannel chan MRChanData)

	CustomReduce(key string, values []interface{}, reduceOutChannel chan MRChanData)
}

type mapReduce struct {
	mapInChannel     chan MRChanData
	mapOutChannel    chan MRChanData
	reduceInChannel  chan MRChanData
	reduceOutChannel chan MRChanData
	customMapReduce  *userCustomMapReduce

	aggregate map[string][]interface{}
	//reduce start Critical Value
	reduceCriticalValue int
	maxMapperNum        int
	maxReducerNum       int
}

func NewMapReduce(uc userCustomMapReduce, reduceCriticalValue int, maxMapper int, maxReducer int) *mapReduce {
	varMapReduce := &mapReduce{
		mapInChannel:        make(chan MRChanData),
		mapOutChannel:       make(chan MRChanData),
		reduceInChannel:     make(chan MRChanData),
		reduceOutChannel:    make(chan MRChanData),
		aggregate:           make(map[string][]interface{}),
		customMapReduce:     &uc,
		reduceCriticalValue: reduceCriticalValue,
		maxMapperNum:        maxMapper,
		maxReducerNum:       maxReducer,
	}
	return varMapReduce
}

func (varMapReduce *mapReduce) GetMapInChannel() (mapInChannel chan MRChanData) {
	return varMapReduce.mapInChannel
}

func (varMapReduce *mapReduce) GetreduceOutChannel() (mapInChannel chan MRChanData) {
	return varMapReduce.reduceOutChannel
}

func (varMapReduce *mapReduce) listenMapIn() {
	var mapWaitGroup sync.WaitGroup
	// start X goroutines do map
	for i := 0; i < varMapReduce.maxMapperNum; i++ {
		mapWaitGroup.Add(1)
		go func() {
			for mapInData := range varMapReduce.mapInChannel {
				(*varMapReduce.customMapReduce).CustomMap(mapInData.Value, varMapReduce.mapOutChannel)
			}
			mapWaitGroup.Done()
		}()
	}
	//when all map task done close mapOut channel
	mapWaitGroup.Wait()
	close(varMapReduce.mapOutChannel)
}

func (varMapReduce *mapReduce) aggregateMapOut() {
	for mapOutData := range varMapReduce.mapOutChannel {
		if len(varMapReduce.aggregate[mapOutData.Key]) == varMapReduce.reduceCriticalValue {
			varMapReduce.reduceInChannel <- MRChanData{mapOutData.Key, varMapReduce.aggregate[mapOutData.Key]}
			varMapReduce.aggregate[mapOutData.Key] = varMapReduce.aggregate[mapOutData.Key][:0]
		}
		varMapReduce.aggregate[mapOutData.Key] = append(varMapReduce.aggregate[mapOutData.Key], mapOutData.Value)
	}
	//when mapOutChannel close,send the rest of data to do reduce
	for k := range varMapReduce.aggregate {
		varMapReduce.reduceInChannel <- MRChanData{k, varMapReduce.aggregate[k]}
	}

	close(varMapReduce.reduceInChannel)
}

func (varMapReduce *mapReduce) listenReduceIn() {
	var reduceWaitGroup sync.WaitGroup
	// start X goroutines to do reduce
	for i := 0; i < varMapReduce.maxReducerNum; i++ {
		reduceWaitGroup.Add(1)
		go func() {
			for reduceInData := range varMapReduce.reduceInChannel {
				(*varMapReduce.customMapReduce).CustomReduce(reduceInData.Key, reduceInData.Value.([]interface{}), varMapReduce.reduceOutChannel)
			}
			reduceWaitGroup.Done()
		}()
	}
	//when all reducer goroutine done close reduceOut channel
	reduceWaitGroup.Wait()
	close(varMapReduce.reduceOutChannel)
}

func (varMapReduce *mapReduce) Run() {

	go varMapReduce.listenMapIn()

	go varMapReduce.aggregateMapOut()

	go varMapReduce.listenReduceIn()

}
