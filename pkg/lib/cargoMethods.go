// methods for CargoInfo

package lib

import (
	"context"
	"strconv"

	"github.com/phayes/freeport"
	"google.golang.org/grpc"

	"github.com/ArmadaStore/cargo/internal/utils"
	"github.com/ArmadaStore/cargo/pkg/cmd"
	cargoToMgr "github.com/ArmadaStore/comms/rpc/cargoToMgr"
)

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
}

func Init(cargoMgrIP string, cargoMgrPort string, volSize string) *CargoInfo {
	var cargoInfo CargoInfo

	cargoInfo.CargoMgrIP = cargoMgrIP
	cargoInfo.CargoMgrPort = cargoMgrPort
	TSize, err := strconv.ParseFloat(volSize, 64)
	cargoInfo.TSize = TSize
	cargoInfo.RSize = float64(0)

	cargoInfo.PublicIP = utils.GetPublicIP()

	port, err := freeport.GetFreePort()
	cargoInfo.Port = int64(port)
	cmd.CheckError(err)

	lat, lon, ip := utils.GetLocationInfo(cargoInfo.PublicIP)
	cargoInfo.Lat = lat
	cargoInfo.Lon = lon
	cargoInfo.PublicIP = ip

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
