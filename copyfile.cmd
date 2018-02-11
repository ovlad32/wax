rem slave1

curl -d "sourceFile=config.json&targetFile=config.copy&targetNode=slave1" -X POST http://localhost:9200/util/copyfile
rem curl -d "sourceFile=\home\vlad\data.GE_P02\data\52\1\ORCL.GE_P2_GRP01_DST01.GL_ATTRIBUTE_MULTI.dat&targetFile=1&targetNode=slave1" -X POST http://localhost:9200/util/copyfile