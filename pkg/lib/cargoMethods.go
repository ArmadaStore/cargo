// methods for CargoInfo

package lib

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"

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
	nReplicas    int
	cargoIDs     []string
	replicaIPs   []string
	replicaPorts []string
	mutex        *sync.Mutex
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

	AppInfo     map[string]ApplicationInfo
	ReplicaChan chan ReplicaData

	CRC map[string]CargoReplicaComm
	CMC CargoMgrComm
	TTC TaskToCargoComm
	CTC CargoToCargoComm
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////
func Init(cargoMgrIP string, cargoMgrPort string, cargoPort string, volSize string) *CargoInfo {
	var cargoInfo CargoInfo

	synth := false
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

	if synth {
		cargoInfo.PublicIP = "127.0.0.1"
	} else {
		cargoInfo.PublicIP = utils.GetPublicIP()
	}
	synth = true
	lat, lon := utils.GetLocationInfo(cargoInfo.PublicIP, synth)
	cargoInfo.Lat = lat
	cargoInfo.Lon = lon

	cargoInfo.AppInfo = make(map[string]ApplicationInfo)
	cargoInfo.ReplicaChan = make(chan ReplicaData)
	cargoInfo.CRC = make(map[string]CargoReplicaComm)

	cargoInfo.TTC.cargoInfo = &cargoInfo
	logTime()
	fmt.Fprintf(os.Stderr, "IP:%s --- Port: %d", cargoInfo.PublicIP, cargoInfo.Port)
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

	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cargoInfo.PublicIP, cargoInfo.Port))
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
	ctc.cargoInfo.WriteToFile(appID, fileName, string(fileBuffer), int(fileSize))

	logTime()
	fmt.Fprintf(os.Stderr, "Written data locally\n")

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
			ack, err := service.StoreInReplica(context.Background(), &sendReplicaData)
			cmd.CheckError(err)

			logTime()
			fmt.Fprintf(os.Stderr, "%s\n", ack)
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

		newAppInfo := ApplicationInfo{
			AppID:        appID,
			nReplicas:    0,
			cargoIDs:     replicaInfo.GetCargoID(),
			replicaIPs:   replicaInfo.GetIP(),
			replicaPorts: replicaInfo.GetPort(),
			mutex:        new(sync.Mutex),
		}
		newAppInfo.nReplicas = len(newAppInfo.replicaIPs)
		ttc.cargoInfo.AppInfo[appID] = newAppInfo
	}
	ttc.cargoInfo.WriteToFile(appID, fileName, string(fileBuffer), int(fileSize))
	ttc.cargoInfo.ReplicaChan <- ReplicaData{fileName: fileName, appID: appID}

	return &taskToCargo.Ack{Ack: "Stored data"}, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////

func (cargoInfo *CargoInfo) WriteToFile(appID string, fileName string, content string, writeSize int) {
	cargoInfo.AppInfo[appID].mutex.Lock()
	//mu := cargoInfo.AppInfo[appID].mutex
	// mu.Lock()
	fileH, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	cmd.CheckError(err)

	writtenSize, err := fileH.WriteString(content)
	cmd.CheckError(err)

	if writtenSize != writeSize {
		cmd.CheckError(errors.New("Written size not same as content size"))
	}

	fileH.Close()
	cargoInfo.AppInfo[appID].mutex.Unlock()
	// mu.Unlock()
}

func (ctc *CargoToCargoComm) WriteInReplica(ctx context.Context, rd *cargoToCargo.ReplicaData) (*cargoToCargo.Ack, error) {
	fileName := rd.GetFileName()
	fileBuffer := rd.GetFileBuffer()
	appID := rd.GetAppID()
	fileSize := rd.GetFileSize()
	//fileType := dts.GetFileType()

	ctc.cargoInfo.WriteToFile(appID, fileName, string(fileBuffer), int(fileSize))

	logTime()
	fmt.Fprintf(os.Stderr, "Written data locally\n")

	return &cargoToCargo.Ack{Ack: "Stored data"}, nil
}

func (cargoInfo *CargoInfo) WriteToReplicas(replicaData cargoToCargo.ReplicaData) {
	appInfo := cargoInfo.AppInfo[replicaData.AppID]
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

		ack, err := service.WriteInReplica(context.Background(), &replicaData)
		cmd.CheckError(err)

		logTime()
		fmt.Fprintf(os.Stderr, "%s\n", ack)
	}

}

func (cargoInfo *CargoInfo) ReadFromFile(appID string, fileName string) string {
	mu := cargoInfo.AppInfo[appID].mutex
	mu.Lock()
	readBytes, err := ioutil.ReadFile(fileName)
	cmd.CheckError(err)
	mu.Unlock()

	return string(readBytes)
}

func (ttc *TaskToCargoComm) WriteToCargo(ctx context.Context, wtc *taskToCargo.WriteData) (*taskToCargo.Ack, error) {
	appID := wtc.GetAppID()
	fileName := wtc.GetFileName()
	fileBuffer := wtc.GetFileBuffer()
	writeSize := int(wtc.GetWriteSize())
	//fileSize := dts.GetFileSize()
	//fileType := dts.GetFileType()

	// replicas send
	if _, ok := ttc.cargoInfo.AppInfo[appID]; ok {

	} else {
		// type assertion
		service := ttc.cargoInfo.CMC.service.(cargoToMgr.RpcCargoToMgrClient)
		replicaInfo, err := service.GetReplicaInfo(context.Background(), &cargoToMgr.AppInfo{AppID: appID})
		cmd.CheckError(err)

		newAppInfo := ApplicationInfo{
			AppID:        appID,
			nReplicas:    0,
			cargoIDs:     replicaInfo.GetCargoID(),
			replicaIPs:   replicaInfo.GetIP(),
			replicaPorts: replicaInfo.GetPort(),
			mutex:        new(sync.Mutex),
		}
		newAppInfo.nReplicas = len(newAppInfo.replicaIPs)
		ttc.cargoInfo.AppInfo[appID] = newAppInfo
	}
	ttc.cargoInfo.WriteToFile(appID, fileName, string(fileBuffer), writeSize)

	replicaData := cargoToCargo.ReplicaData{
		FileName:   fileName,
		FileBuffer: fileBuffer,
		FileSize:   int64(writeSize),
		FileType:   "txt",
		AppID:      appID,
	}

	service := ttc.cargoInfo.CMC.service.(cargoToMgr.RpcCargoToMgrClient)

	// Strong consistency
	// Lock in Cargo Manager
	_, err := service.AcquireWriteLock(context.Background(), &cargoToMgr.AppInfo{AppID: appID})
	cmd.CheckError(err)
	ttc.cargoInfo.WriteToReplicas(replicaData)
	_, err = service.ReleaseWriteLock(context.Background(), &cargoToMgr.AppInfo{AppID: appID})
	cmd.CheckError(err)

	return &taskToCargo.Ack{Ack: "Stored data"}, nil
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
