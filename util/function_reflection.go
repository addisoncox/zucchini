package util

import (
	"fmt"
	"reflect"

	"github.com/addisoncox/zucchini/task"
)

func functionArgumentsMatchSignature(function interface{}, args ...interface{}) bool {
	functionType := reflect.TypeOf(function)
	functionArity := functionType.NumIn()

	if functionType.IsVariadic() {
		if len(args) < functionArity {
			return false
		}
	} else {
		if len(args) != functionArity {
			return false
		}
	}

	for i := 0; i < functionArity; i++ {
		if functionType.In(i).String() == "interface {}" {
			continue
		} else if functionType.In(i).String() == "[]interface {}" {
			// no further checks can be done - accept anything
			return true
		}
		if functionType.In(i) != reflect.TypeOf(args[i]) {
			return false
		}
	}
	return true
}

func Call(function interface{}, arguments ...interface{}) task.TaskResult {
	functionType := reflect.TypeOf(function)

	if functionType.Kind() != reflect.Func {
		panic("Attempted to call non function")
	}

	if !functionArgumentsMatchSignature(function, arguments...) {
		panic("Function arguments did not match signature")
	}

	functionValue := reflect.ValueOf(function)

	argsIn := make([]reflect.Value, 0)

	for _, arg := range arguments {
		argsIn = append(argsIn, reflect.ValueOf(arg))
	}

	functionResult := functionValue.Call(argsIn)
	if functionResult == nil {
		return task.TaskResult{
			Status: task.TaskSucceeded,
			Value:  "",
		}
	}
	return task.TaskResult{
		Status: task.TaskSucceeded,
		Value:  fmt.Sprintf("%v", functionResult[0].Interface()),
	}
}
