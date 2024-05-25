package helpers

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/EmanuelPutura/distributed_algo/protobuf"
)

func RetrieveIdFromAbstraction(abstractionId string) string {
	re := regexp.MustCompile(`\[(.*)\]`)
	tokens := re.FindStringSubmatch(abstractionId)
	return tokens[1]
}

func GetProcessName(process *protobuf.ProcessId) string {
	return fmt.Sprintf("%s-%s", process.Owner, strconv.Itoa(int(process.Index)))
}
