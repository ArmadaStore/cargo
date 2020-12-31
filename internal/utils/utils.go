package utils

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

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
func GetLocationInfo(ip string, synth bool) (float64, float64) {

	lat := float64(0)
	lon := float64(0)
	if synth {
		rand.Seed(time.Now().UnixNano())
		csvfile, err := os.Open("internal/utils/latlon.csv")
		if err != nil {
			log.Fatalln("Error: Can't open file", err)
		}
		defer csvfile.Close()

		r := csv.NewReader(csvfile)
		randLineNumber := rand.Intn(100) + 1
		currLineNum := 1
		for {
			record, err := r.Read()
			cmd.CheckError(err)
			if err == io.EOF {
				break
			}

			if currLineNum != randLineNumber {
				currLineNum++
				continue
			} else {
				// fmt.Println(record)
				lat, err = strconv.ParseFloat(record[0], 64)
				cmd.CheckError(err)

				lon, err = strconv.ParseFloat(record[1], 64)
				cmd.CheckError(err)
			}
			break
		}

		return lat, lon
	} else {
		resp, err := http.Get("http://api.ipstack.com/" + ip + "?access_key=0b4b060808fd3d2eca19a17594008fdb")
		cmd.CheckError(err)
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		cmd.CheckError(err)

		var geoLocInfo GeoLocInfo
		err = json.Unmarshal(body, &geoLocInfo)
		cmd.CheckError(err)

		return geoLocInfo.Lat, geoLocInfo.Lon
	}

}
