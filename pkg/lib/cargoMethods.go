// methods for CargoInfo

package lib

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ArmadaStore/cargo/internal/utils"
	"github.com/ArmadaStore/cargo/pkg/cmd"
	"github.com/ArmadaStore/comms/rpc/cargoToCargo"
	"github.com/ArmadaStore/comms/rpc/cargoToMgr"
	"github.com/ArmadaStore/comms/rpc/taskToCargo"
)

type TaskToCargoComm struct {
	taskToCargo.UnimplementedRpcTaskToCargoServer

	cargoInfo *CargoInfo
}

type CargoToCargoComm struct {
	cargoToCargo.UnimplementedRpcCargoToCargoServer

	cargoInfo *CargoInfo
}

type ApplicationInfo struct {
	AppID        string
	cType        string
	nReplicas    int
	cargoIDs     []string
	replicaIPs   []string
	replicaPorts []string
	mutex        *sync.Mutex
	currPos      int64
}

type CargoReplicaComm struct {
	cc      *grpc.ClientConn
	service interface{}
}

type CargoMgrComm struct {
	cc      *grpc.ClientConn
	service interface{}
}

type ReplicaData struct {
	fileName string
	appID    string
}

type CargoInfo struct {
	ID           string
	PublicIP     string
	Port         int64
	CargoMgrIP   string
	CargoMgrPort string
	Lat          float64
	Lon          float64
	TSize        float64
	RSize        float64
	mutex        *sync.Mutex

	AppInfo        map[string]*ApplicationInfo
	ReplicaChannel chan cargoToCargo.ReplicaData
	ReplicaChan    chan ReplicaData

	CRC map[string]CargoReplicaComm
	CMC CargoMgrComm
	TTC TaskToCargoComm
	CTC CargoToCargoComm
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////
func Init(cargoMgrIP string, cargoMgrPort string, cargoPort string, volSize string) *CargoInfo {
	var cargoInfo CargoInfo

	synth := true
	cargoInfo.CargoMgrIP = cargoMgrIP
	cargoInfo.CargoMgrPort = cargoMgrPort
	TSize, err := strconv.ParseFloat(volSize, 64)
	cargoInfo.TSize = TSize
	cargoInfo.RSize = float64(0)

	// port, err := freeport.GetFreePort()
	// cargoInfo.Port = int64(port)
	// cmd.CheckError(err)

	cargoInfo.Port, err = strconv.ParseInt(cargoPort, 10, 64)
	cmd.CheckError(err)

	// if synth {
	// cargoInfo.PublicIP = "0.0.0.0"
	// } else {
	cargoInfo.PublicIP = utils.GetPublicIP()
	// }
	// synth = true
	lat, lon := utils.GetLocationInfo(cargoInfo.PublicIP, synth)
	cargoInfo.Lat = lat
	cargoInfo.Lon = lon

	cargoInfo.mutex = new(sync.Mutex)
	cargoInfo.AppInfo = make(map[string]*ApplicationInfo)
	cargoInfo.ReplicaChan = make(chan ReplicaData)
	cargoInfo.ReplicaChannel = make(chan cargoToCargo.ReplicaData)
	cargoInfo.CRC = make(map[string]CargoReplicaComm)

	cargoInfo.TTC.cargoInfo = &cargoInfo
	cargoInfo.CTC.cargoInfo = &cargoInfo
	log.Println("IP: ", cargoInfo.PublicIP, "--- Port:", cargoInfo.Port)
	return &cargoInfo
}

func (cargoInfo *CargoInfo) Register() {
	conn, err := grpc.Dial(cargoInfo.CargoMgrIP+":"+cargoInfo.CargoMgrPort, grpc.WithInsecure())
	cmd.CheckError(err)

	cargoInfo.CMC.cc = conn

	cargoInfo.CMC.service = cargoToMgr.NewRpcCargoToMgrClient(conn)
	sendCargoInfo := cargoToMgr.CargoInfo{
		IP:    cargoInfo.PublicIP,
		Port:  strconv.Itoa(int(cargoInfo.Port)),
		TSize: cargoInfo.TSize,
		Lat:   cargoInfo.Lat,
		Lon:   cargoInfo.Lon,
	}

	// type assertion
	service := cargoInfo.CMC.service.(cargoToMgr.RpcCargoToMgrClient)
	ack, err := service.RegisterToMgr(context.Background(), &sendCargoInfo)
	cmd.CheckError(err)

	cargoInfo.ID = ack.GetID()
}

func (cargoInfo *CargoInfo) ListenTasks(wg *sync.WaitGroup) {
	defer wg.Done()

	listen, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", cargoInfo.Port))
	cmd.CheckError(err)

	server := grpc.NewServer()
	taskToCargo.RegisterRpcTaskToCargoServer(server, &(cargoInfo.TTC))
	cargoToCargo.RegisterRpcCargoToCargoServer(server, &(cargoInfo.CTC))

	reflection.Register(server)

	err = server.Serve(listen)
	cmd.CheckError(err)
}

func (cargoInfo *CargoInfo) CleanUp() {
	cargoInfo.CMC.cc.Close()

}

////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////

func (ctc *CargoToCargoComm) StoreInReplica(ctx context.Context, rd *cargoToCargo.ReplicaData) (*cargoToCargo.Ack, error) {
	appID := rd.GetAppID()
	fileName := rd.GetFileName()
	fileBuffer := rd.GetFileBuffer()
	fileSize := rd.GetFileSize()
	//fileType := dts.GetFileType()

	// err := ioutil.WriteFile(fileName, fileBuffer, 0644)
	// cmd.CheckError(err)
	ctc.cargoInfo.WriteToFile(appID, fileName, string(fileBuffer), int(fileSize), true)

	log.Println("Written data locally")

	return &cargoToCargo.Ack{Ack: "Stored data"}, nil
}

func (cargoInfo *CargoInfo) SendToReplicas() {
	for {
		replicaInfo := <-cargoInfo.ReplicaChan
		appInfo := cargoInfo.AppInfo[replicaInfo.appID]
		for i := 0; i < len(appInfo.cargoIDs); i++ {
			var service cargoToCargo.RpcCargoToCargoClient
			if crc, ok := cargoInfo.CRC[appInfo.cargoIDs[i]]; ok {
				service = crc.service.(cargoToCargo.RpcCargoToCargoClient)

			} else {
				IP := appInfo.replicaIPs[i]
				Port := appInfo.replicaPorts[i]
				conn, err := grpc.Dial(IP+":"+Port, grpc.WithInsecure())
				cmd.CheckError(err)

				cargoInfo.CRC[appInfo.cargoIDs[i]] = CargoReplicaComm{
					cc:      conn,
					service: cargoToCargo.NewRpcCargoToCargoClient(conn),
				}
				service = cargoInfo.CRC[appInfo.cargoIDs[i]].service.(cargoToCargo.RpcCargoToCargoClient)
			}

			fileBuf, err := ioutil.ReadFile(replicaInfo.fileName)
			cmd.CheckError(err)

			sendReplicaData := cargoToCargo.ReplicaData{
				FileName:   replicaInfo.fileName,
				FileBuffer: fileBuf,
				FileSize:   int64(len(fileBuf)),
				FileType:   filepath.Ext(replicaInfo.fileName),
				AppID:      replicaInfo.appID,
			}
			_, err = service.StoreInReplica(context.Background(), &sendReplicaData)
			cmd.CheckError(err)
		}

	}

}
func (ttc *TaskToCargoComm) LoadFromCargo(ctx context.Context, fileInfo *taskToCargo.FileInfo) (*taskToCargo.DataToLoad, error) {
	fileName := fileInfo.GetFileName()
	fileBuf, err := ioutil.ReadFile(fileName)
	cmd.CheckError(err)

	fileSize := len(fileBuf)
	fileType := filepath.Ext(fileName)

	return &taskToCargo.DataToLoad{
		FileName:   fileName,
		FileBuffer: fileBuf,
		FileSize:   int64(fileSize),
		FileType:   fileType,
	}, nil
}

func (ttc *TaskToCargoComm) StoreInCargo(ctx context.Context, dts *taskToCargo.DataToStore) (*taskToCargo.Ack, error) {
	appID := dts.GetAppID()
	fileName := dts.GetFileName()
	fileBuffer := dts.GetFileBuffer()
	fileSize := dts.GetFileSize()
	//fileType := dts.GetFileType()

	// err := ioutil.WriteFile(fileName, fileBuffer, 0644)
	// cmd.CheckError(err)

	// replicas send
	if _, ok := ttc.cargoInfo.AppInfo[appID]; ok {

	} else {
		// type assertion
		service := ttc.cargoInfo.CMC.service.(cargoToMgr.RpcCargoToMgrClient)
		replicaInfo, err := service.GetReplicaInfo(context.Background(), &cargoToMgr.AppInfo{AppID: appID})
		cmd.CheckError(err)

		newAppInfo := new(ApplicationInfo)
		newAppInfo.AppID = appID
		newAppInfo.cType = replicaInfo.GetCType()
		newAppInfo.cargoIDs = replicaInfo.GetCargoID()
		newAppInfo.replicaIPs = replicaInfo.GetIP()
		newAppInfo.replicaPorts = replicaInfo.GetPort()
		newAppInfo.mutex = new(sync.Mutex)
		newAppInfo.currPos = 0
		newAppInfo.nReplicas = len(newAppInfo.replicaIPs)
		ttc.cargoInfo.AppInfo[appID] = newAppInfo
	}
	ttc.cargoInfo.WriteToFile(appID, fileName, string(fileBuffer), int(fileSize), false)
	ttc.cargoInfo.ReplicaChan <- ReplicaData{fileName: fileName, appID: appID}

	return &taskToCargo.Ack{Ack: "Stored data"}, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////

func (cargoInfo *CargoInfo) WriteToFile(appID string, fileName string, content string, writeSize int, isReplica bool) {
	start := time.Now()
	// fmt.Println("Before write lock: ", cargoInfo.AppInfo[appID].mutex)
	cargoInfo.AppInfo[appID].mutex.Lock()
	// fmt.Println("After write lock: ", cargoInfo.AppInfo[appID].mutex)

	fileH, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	cmd.CheckError(err)

	writtenSize, err := fileH.WriteString(content)
	cmd.CheckError(err)

	if writtenSize != writeSize {
		cmd.CheckError(errors.New("Written size not same as content size"))
	}

	if !isReplica {
		fileInfo, err := fileH.Stat()
		if err != nil {
			log.Fatalf("File stat error")
		}
		cargoInfo.AppInfo[appID].currPos = fileInfo.Size()
	}

	fileH.Close()
	// fmt.Println("Before write unlock: ", cargoInfo.AppInfo[appID].mutex)
	cargoInfo.AppInfo[appID].mutex.Unlock()
	// fmt.Println("After write unlock: ", cargoInfo.AppInfo[appID].mutex)
	end := time.Since(start)
	log.Println("Write latency: ", end)
}

func (ctc *CargoToCargoComm) WriteInReplica(ctx context.Context, rd *cargoToCargo.ReplicaData) (*cargoToCargo.Ack, error) {
	fileName := rd.GetFileName()
	fileBuffer := rd.GetFileBuffer()
	appID := rd.GetAppID()
	fileSize := rd.GetFileSize()
	//fileType := dts.GetFileType()

	ctc.cargoInfo.mutex.Lock()
	if _, ok := ctc.cargoInfo.AppInfo[appID]; ok {

	} else {
		// type assertion
		service := ctc.cargoInfo.CMC.service.(cargoToMgr.RpcCargoToMgrClient)
		replicaInfo, err := service.GetReplicaInfo(context.Background(), &cargoToMgr.AppInfo{AppID: appID})
		cmd.CheckError(err)

		newAppInfo := new(ApplicationInfo)
		newAppInfo.AppID = appID
		newAppInfo.cType = replicaInfo.GetCType()
		newAppInfo.cargoIDs = replicaInfo.GetCargoID()
		newAppInfo.replicaIPs = replicaInfo.GetIP()
		newAppInfo.replicaPorts = replicaInfo.GetPort()
		newAppInfo.mutex = new(sync.Mutex)
		newAppInfo.currPos = 0
		newAppInfo.nReplicas = len(newAppInfo.replicaIPs)

		ctc.cargoInfo.AppInfo[appID] = newAppInfo
	}
	ctc.cargoInfo.mutex.Unlock()

	ctc.cargoInfo.WriteToFile(appID, fileName, string(fileBuffer), int(fileSize), true)

	log.Println("WRITE COMPLETE")

	return &cargoToCargo.Ack{Ack: "Stored data"}, nil
}

func (cargoInfo *CargoInfo) StrongWriteToReplicas(replicaData cargoToCargo.ReplicaData) {
	var wg sync.WaitGroup

	appInfo := cargoInfo.AppInfo[replicaData.AppID]
	for i := 0; i < len(appInfo.cargoIDs); i++ {
		if appInfo.cargoIDs[i] == cargoInfo.ID {
			continue
		}
		var service cargoToCargo.RpcCargoToCargoClient
		if crc, ok := cargoInfo.CRC[appInfo.cargoIDs[i]]; ok {
			service = crc.service.(cargoToCargo.RpcCargoToCargoClient)

		} else {
			IP := appInfo.replicaIPs[i]
			Port := appInfo.replicaPorts[i]
			conn, err := grpc.Dial(IP+":"+Port, grpc.WithInsecure())
			cmd.CheckError(err)

			cargoInfo.CRC[appInfo.cargoIDs[i]] = CargoReplicaComm{
				cc:      conn,
				service: cargoToCargo.NewRpcCargoToCargoClient(conn),
			}
			service = cargoInfo.CRC[appInfo.cargoIDs[i]].service.(cargoToCargo.RpcCargoToCargoClient)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := service.WriteInReplica(context.Background(), &replicaData)
			cmd.CheckError(err)
			// log.Println("WRITE COMPLETE in ", appInfo.replicaIPs[i], ":", appInfo.replicaPorts[i])
		}()
	}
	wg.Wait()
}

func (cargoInfo *CargoInfo) WriteToReplicas() {
	var wg sync.WaitGroup
	for {
		replicaData := <-cargoInfo.ReplicaChannel
		appInfo := cargoInfo.AppInfo[replicaData.AppID]
		for i := 0; i < len(appInfo.cargoIDs); i++ {
			if appInfo.cargoIDs[i] == cargoInfo.ID {
				continue
			}
			var service cargoToCargo.RpcCargoToCargoClient
			if crc, ok := cargoInfo.CRC[appInfo.cargoIDs[i]]; ok {
				service = crc.service.(cargoToCargo.RpcCargoToCargoClient)

			} else {
				IP := appInfo.replicaIPs[i]
				Port := appInfo.replicaPorts[i]
				conn, err := grpc.Dial(IP+":"+Port, grpc.WithInsecure())
				cmd.CheckError(err)

				cargoInfo.CRC[appInfo.cargoIDs[i]] = CargoReplicaComm{
					cc:      conn,
					service: cargoToCargo.NewRpcCargoToCargoClient(conn),
				}
				service = cargoInfo.CRC[appInfo.cargoIDs[i]].service.(cargoToCargo.RpcCargoToCargoClient)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := service.WriteInReplica(context.Background(), &replicaData)
				cmd.CheckError(err)
				// log.Println("WRITE COMPLETE in ", appInfo.replicaIPs[i], ":", appInfo.replicaPorts[i])
			}()
		}
	}
	wg.Wait()
}

func (ttc *TaskToCargoComm) WriteToCargo(ctx context.Context, wtc *taskToCargo.WriteData) (*taskToCargo.Ack, error) {
	appID := wtc.GetAppID()
	fileName := wtc.GetFileName()
	fileBuffer := wtc.GetFileBuffer()
	writeSize := int(wtc.GetWriteSize())
	//fileSize := dts.GetFileSize()
	//fileType := dts.GetFileType()

	// replicas send
	ttc.cargoInfo.mutex.Lock()
	if _, ok := ttc.cargoInfo.AppInfo[appID]; ok {

	} else {
		// type assertion
		service := ttc.cargoInfo.CMC.service.(cargoToMgr.RpcCargoToMgrClient)
		replicaInfo, err := service.GetReplicaInfo(context.Background(), &cargoToMgr.AppInfo{AppID: appID})
		cmd.CheckError(err)

		newAppInfo := new(ApplicationInfo)
		newAppInfo.AppID = appID
		newAppInfo.cType = replicaInfo.GetCType()
		newAppInfo.cargoIDs = replicaInfo.GetCargoID()
		newAppInfo.replicaIPs = replicaInfo.GetIP()
		newAppInfo.replicaPorts = replicaInfo.GetPort()
		newAppInfo.mutex = new(sync.Mutex)
		newAppInfo.currPos = 0
		newAppInfo.nReplicas = len(newAppInfo.replicaIPs)

		ttc.cargoInfo.AppInfo[appID] = newAppInfo
	}
	ttc.cargoInfo.mutex.Unlock()
	ttc.cargoInfo.WriteToFile(appID, fileName, string(fileBuffer), writeSize, false)

	replicaData := cargoToCargo.ReplicaData{
		FileName:   fileName,
		FileBuffer: fileBuffer,
		FileSize:   int64(writeSize),
		FileType:   "txt",
		AppID:      appID,
	}

	// service := ttc.cargoInfo.CMC.service.(cargoToMgr.RpcCargoToMgrClient)

	start := time.Now()
	// Strong consistency
	// Lock in Cargo Manager
	// _, err := service.AcquireWriteLock(context.Background(), &cargoToMgr.AppInfo{AppID: appID})
	// cmd.CheckError(err)
	if ttc.cargoInfo.AppInfo[appID].cType == "STRONG" {
		ttc.cargoInfo.StrongWriteToReplicas(replicaData)
	} else {
		ttc.cargoInfo.ReplicaChannel <- replicaData
	}
	// _, err = service.ReleaseWriteLock(context.Background(), &cargoToMgr.AppInfo{AppID: appID})
	// cmd.CheckError(err)
	end := time.Since(start)
	log.Println("Total replica write latency: ", end)

	return &taskToCargo.Ack{Ack: "Stored data"}, nil
}

func (cargoInfo *CargoInfo) ReadFromFile(appID string, fileName string) string {
	start := time.Now()
	// fmt.Println("Before read lock: ", cargoInfo.AppInfo[appID].mutex)
	cargoInfo.AppInfo[appID].mutex.Lock()
	// fmt.Println("After read lock: ", cargoInfo.AppInfo[appID].mutex)

	fileH, err := os.Open(fileName)
	cmd.CheckError(err)

	fileInfo, err := fileH.Stat()
	if err != nil {
		log.Fatalf("File stat error")
	}

	readSize := fileInfo.Size() - cargoInfo.AppInfo[appID].currPos
	// fmt.Println("ReadSize: ", readSize)
	if readSize < 5 {
		// fmt.Println("Before read unlock: ", cargoInfo.AppInfo[appID].mutex)
		cargoInfo.AppInfo[appID].mutex.Unlock()
		// fmt.Println("After read unlock: ", cargoInfo.AppInfo[appID].mutex)
		return string("")
	}
	_, err = fileH.Seek(cargoInfo.AppInfo[appID].currPos, 0)
	if err != nil {
		log.Fatalf("Seek error")
	}
	readBytes := make([]byte, readSize)
	_, err = fileH.Read(readBytes)
	if err != nil {
		log.Fatalf("Read error")
	}
	cargoInfo.AppInfo[appID].currPos = fileInfo.Size()
	// fmt.Println("Before read unlock: ", cargoInfo.AppInfo[appID].mutex)
	cargoInfo.AppInfo[appID].mutex.Unlock()
	// fmt.Println("After read unlock: ", cargoInfo.AppInfo[appID].mutex)
	end := time.Since(start)

	log.Println("Read Latency: ", end)
	return string(readBytes)
}

func (ttc *TaskToCargoComm) ReadFromCargo(ctx context.Context, readInfo *taskToCargo.ReadInfo) (*taskToCargo.ReadData, error) {
	appID := readInfo.GetAppID()
	fileName := readInfo.GetFileName()

	content := ttc.cargoInfo.ReadFromFile(appID, fileName)

	fileSize := len(content)

	return &taskToCargo.ReadData{
		FileName:   fileName,
		FileBuffer: []byte(content),
		ReadSize:   int64(fileSize),
	}, nil
}
