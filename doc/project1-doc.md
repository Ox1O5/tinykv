# 任务分解
1. 实现一个单机引擎
gRPC初始化在kv/main.go  
1.1 实现 /kv/storage/standalone_storage/
    * 封装badger的事务的API
    *
2. 实现一个行kv服务业务

TestRawGetNotFound1
Error是不是应该返回Key not found
错误处理 有的为nil 有的返回err