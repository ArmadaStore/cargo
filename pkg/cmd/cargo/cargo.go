// publicly availble code for cargo entry

package cargo

import (
	"errors"
	"fmt"
	"os"
)

func Run(cargoMgrIP string, cargoMgrPort string) error {
	fmt.Fprint(os.Stderr, "Entering Run Entry point\n")
	err := errors.New("Error")
	return err
}
