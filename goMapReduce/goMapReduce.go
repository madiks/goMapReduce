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
	tokens := make(chan int, varMapReduce.maxMapperNum)
	for mapInData := range varMapReduce.mapInChannel {
		//when get task data from channel, try get token, if success start one mapper, else wait other mapper quit, max mapper count is maxMapperNum
		tokens <- 1
		mapWaitGroup.Add(1)
		go func() {
			(*varMapReduce.customMapReduce).CustomMap(mapInData.Value, varMapReduce.mapOutChannel)
			mapWaitGroup.Done()
			<-tokens
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
	tokens := make(chan int, varMapReduce.maxReducerNum)
	for reduceInData := range varMapReduce.reduceInChannel {
		//when get task data from channel, try get token, if success start one reducer, else wait other reducer quit, max reducer count is maxReducerNum
		tokens <- 1
		reduceWaitGroup.Add(1)
		go func() {
			(*varMapReduce.customMapReduce).CustomReduce(reduceInData.Key, reduceInData.Value.([]interface{}), varMapReduce.reduceOutChannel)
			reduceWaitGroup.Done()
			<-tokens
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
