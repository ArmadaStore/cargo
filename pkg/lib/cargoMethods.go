// methods for CargoInfo

package lib

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/phayes/freeport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/ArmadaStore/cargo/internal/utils"
	"github.com/ArmadaStore/cargo/pkg/cmd"
	"github.com/ArmadaStore/comms/rpc/cargoToMgr"
	"github.com/ArmadaStore/comms/rpc/taskToCargo"
)

type TaskToCargoComm struct {
	taskToCargo.UnimplementedRpcTaskToCargoServer

	cargoInfo *CargoInfo
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

	TTC TaskToCargoComm
}

func Init(cargoMgrIP string, cargoMgrPort string, volSize string) *CargoInfo {
	var cargoInfo CargoInfo

	synth := true
	cargoInfo.CargoMgrIP = cargoMgrIP
	cargoInfo.CargoMgrPort = cargoMgrPort
	TSize, err := strconv.ParseFloat(volSize, 64)
	cargoInfo.TSize = TSize
	cargoInfo.RSize = float64(0)

	port, err := freeport.GetFreePort()
	cargoInfo.Port = int64(port)
	cmd.CheckError(err)

	if synth {
		cargoInfo.PublicIP = "127.0.0.1"
	} else {
		cargoInfo.PublicIP = utils.GetPublicIP()
	}
	lat, lon := utils.GetLocationInfo(cargoInfo.PublicIP, synth)
	cargoInfo.Lat = lat
	cargoInfo.Lon = lon

	cargoInfo.TTC.cargoInfo = &cargoInfo
	fmt.Fprintf(os.Stderr, "IP:%s --- Port: %d", cargoInfo.PublicIP, cargoInfo.Port)
	return &cargoInfo
}

func (cargoInfo *CargoInfo) Register() {
	conn, err := grpc.Dial(cargoInfo.CargoMgrIP+":"+cargoInfo.CargoMgrPort, grpc.WithInsecure())
	cmd.CheckError(err)
	defer conn.Close()

	service := cargoToMgr.NewRpcCargoToMgrClient(conn)
	sendCargoInfo := cargoToMgr.CargoInfo{
		IP:    cargoInfo.PublicIP,
		Port:  strconv.Itoa(int(cargoInfo.Port)),
		TSize: cargoInfo.TSize,
		Lat:   cargoInfo.Lat,
		Lon:   cargoInfo.Lon,
	}
	ack, err := service.RegisterToMgr(context.Background(), &sendCargoInfo)
	cmd.CheckError(err)

	cargoInfo.ID = ack.GetID()
}

func (ttc *TaskToCargoComm) StoreInCargo(ctx context.Context, dts *taskToCargo.DataToStore) (*taskToCargo.Ack, error) {
	fileName := dts.GetFileName()
	fileBuffer := dts.GetFileBuffer()
	//fileSize := dts.GetFileSize()
	//fileType := dts.GetFileType()

	err := ioutil.WriteFile(fileName, fileBuffer, 0644)
	cmd.CheckError(err)

	return &taskToCargo.Ack{Ack: "Stored data"}, nil
}

func (cargoInfo *CargoInfo) ListenTasks(wg *sync.WaitGroup) {
	defer wg.Done()

	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cargoInfo.PublicIP, cargoInfo.Port))
	cmd.CheckError(err)

	server := grpc.NewServer()
	taskToCargo.RegisterRpcTaskToCargoServer(server, &(cargoInfo.TTC))

	reflection.Register(server)

	err = server.Serve(listen)
	cmd.CheckError(err)
}
