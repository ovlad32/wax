rem slave1

curl -d "sourceFile=config.json&targetFile=config.copy&targetNode=slave1" -X POST http://localhost:9200/util/copyfile