rem slave
set path=%PATH%;c:\gcc\bin;
rem curl -d "sourceFile=./config.json&sourceNodeId=nodeId1&targetFile=./config.copy&targetNodeId=nodeId2" -X POST http://localhost:9200/util/copyfile
curl -d "sourceFile=\home\vlad\data.GE_P02\data\52\1\ORCL.GE_P2_GRP01_DST01.GL_ATTRIBUTE_MULTI.dat&sourceNodeId=nodeId1&targetFile=./1&targetNodeId=nodeId2" -X POST http://localhost:9200/util/copyfile
rem curl -d "sourceFile=\home\vlad\data.GE_P02\data\52\1\ORCL.GE_P2_GRP01_DST01.GL_ATTRIBUTE_MULTI.dat&targetFile=1&targetNode=slave1" -X POST http://localhost:9200/util/copyfile