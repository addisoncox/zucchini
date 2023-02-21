	type Task  [T, R] {
		id [uuid]
		Data		T
		HandlerFunc	func T -> R
	}
	
	
	// 
	client.go 
	import MyTask
	
	queue.append (MyTask.instance(data)) 
	
	//server.go
	
	queue.handle(MyTask) 
