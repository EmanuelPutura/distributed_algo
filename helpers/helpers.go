package helpers

import "regexp"

func RetrieveRegisterFromAbstraction(abstractionId string) string {
	re := regexp.MustCompile(`\[(.*)\]`)
	tokens := re.FindStringSubmatch(abstractionId)
	return tokens[1]
}
