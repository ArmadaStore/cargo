package utils

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/ArmadaStore/cargo/pkg/cmd"
)

type GeoLocInfo struct {
	IP  string  `json:"ip"`
	Lat float64 `json:"latitude"`
	Lon float64 `json:"longitude"`
}

// return the public of the calling node
func GetPublicIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	cmd.CheckError(err)
	defer conn.Close()

	publicIP := conn.LocalAddr().(*net.UDPAddr)

	return publicIP.IP.String()
}

// return the lat, lon of the calling node
func GetLocationInfo(ip string) (float64, float64, string) {

	resp, err := http.Get("http://api.ipstack.com/" + ip + "?access_key=0b4b060808fd3d2eca19a17594008fdb")
	cmd.CheckError(err)
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	cmd.CheckError(err)

	var geoLocInfo GeoLocInfo
	err = json.Unmarshal(body, &geoLocInfo)
	cmd.CheckError(err)

	return geoLocInfo.Lat, geoLocInfo.Lon, ip
}
