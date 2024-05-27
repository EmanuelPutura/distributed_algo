package uc

import (
	"fmt"

	"github.com/EmanuelPutura/distributed_algo/beb"
	"github.com/EmanuelPutura/distributed_algo/consensus/ep"
)

func (uc *UniformConsensus) getId() string {
	return fmt.Sprintf("%s.ep[%d]", uc.abstraction_id, uc.epoch_timestamp)
}

func (uc UniformConsensus) initAbstractions(value_state *ep.EpValueState) *UniformConsensus {
	base_abstraction_id := uc.getId()

	uc.all_abstractions[base_abstraction_id] = ep.Create(
		base_abstraction_id,
		uc.abstraction_id,
		uc.epoch_timestamp,
		value_state,
		uc.messages_queue,
		uc.all_processes,
	)

	beb_abstraction_id := fmt.Sprintf("%s.beb", base_abstraction_id)
	uc.all_abstractions[beb_abstraction_id] = beb.Create(
		beb_abstraction_id,
		uc.messages_queue,
		uc.all_processes,
	)

	uc.all_abstractions[fmt.Sprintf("%s.pl", base_abstraction_id)] = uc.perfect_link.Copy().SetParentAbstraction(base_abstraction_id)
	uc.all_abstractions[fmt.Sprintf("%s.beb.pl", base_abstraction_id)] = uc.perfect_link.Copy().SetParentAbstraction(beb_abstraction_id)

	return &uc
}
