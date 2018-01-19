
rem protoc --go_out=./../hearth/process/categorysplit categorysplitfile.proto 
protoc --go_out=plugins=grpc:./../hearth/grpcservice ./api.proto
