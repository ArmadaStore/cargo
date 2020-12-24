// publicly availble code for cargo entry

package cargo

import (
	"fmt"

	"github.com/ArmadaStore/cargo/pkg/lib"
)

func Run(cargoMgrIP string, cargoMgrPort string, volSize string) error {
	cargoInfo := lib.Init(cargoMgrIP, cargoMgrPort, volSize)
	cargoInfo.Register()
	return fmt.Errorf("Hello")
}
