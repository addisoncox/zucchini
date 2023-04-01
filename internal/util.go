package internal

import "encoding/json"

func UnmarshalOrPanic[T any](data []byte) T {
	var res T
	err := json.Unmarshal(data, &res)
	if err != nil {
		panic(err.Error())
	}
	return res
}
