// publicly availble code for cargo entry

package cargo

import (
	"fmt"
	"sync"

	"github.com/ArmadaStore/cargo/pkg/lib"
)

func Run(cargoMgrIP string, cargoMgrPort string, volSize string) error {
	cargoInfo := lib.Init(cargoMgrIP, cargoMgrPort, volSize)
	cargoInfo.Register()

	var wg sync.WaitGroup
	cargoInfo.ListenTasks(&wg)

	return fmt.Errorf("Hello")
}
