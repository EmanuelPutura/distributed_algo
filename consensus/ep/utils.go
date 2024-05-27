package ep

import "github.com/EmanuelPutura/distributed_algo/protobuf"

type EpValueState struct {
	value_timestamp int32
	value           *protobuf.Value
}

func CreateEpValueState(value_timestamp int32, value *protobuf.Value) *EpValueState {
	return &EpValueState{
		value_timestamp: value_timestamp,
		value:           value,
	}
}

func (ep *EpochConsensus) highestValueState() *EpValueState {
	result := &EpValueState{}
	for _, v := range ep.all_value_states {
		if v.value_timestamp > result.value_timestamp {
			result = v
		}
	}
	return result
}
