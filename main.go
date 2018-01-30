package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/goinggo/tracelog"
	"github.com/ovlad32/wax/hearth"
	"github.com/ovlad32/wax/hearth/appnode"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/ovlad32/wax/hearth/misc"
	"github.com/ovlad32/wax/hearth/process/categorysplit"
	"github.com/ovlad32/wax/hearth/process/index"
	"github.com/ovlad32/wax/hearth/process/search"
	"github.com/ovlad32/wax/hearth/process/sort"
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/sirupsen/logrus"
	"math"
	"os"
	"runtime"
	stdlog "log"
	"runtime/pprof"
	"path"
)

var packageName = "main"

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

var pathToConfigFile = flag.String("configfile", "./config.json", "path to config file")
var argMetadataIds = flag.String("metadata_id", string(-math.MaxInt64), "")
var argWorkflowIds = flag.String("workflow_id", string(-math.MaxInt64), "")

var applicationRole = flag.String("role", "", "")
var masterNodeHost = flag.String("masterHost", "localhost", "")
var masterNodePort = flag.String("masterPort", "9100", "")
var nodeIdDirectory = flag.String("nodeIdDir", "nodeId", "")

func main() {
	flag.Parse()
	tracelog.Start(tracelog.LevelInfo)
	defer tracelog.Stop()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			stdlog.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			stdlog.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			stdlog.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			stdlog.Fatal("could not write memory profile: ", err)
		}
		defer f.Close()
	}
	var err error

	config, err := handling.ReadConfig()
	if err != nil {
		stdlog.Fatal(err)
	}

	config.Logger = logrus.New()

	_, err = repository.InitRepository(hearth.AdaptRepositoryConfig(config))
	if err != nil {
		stdlog.Fatal(err)
	}

	node, err := appnode.NewApplicationNode(
		&appnode.ApplicationNodeConfigType{
			Logger:                config.Logger,
			GrpcPort:              9100,
			RestPort:              9200,
			SlaveHeartBeatSeconds: 10,
		},
	)
	if err != nil {
		stdlog.Fatal(err)
	}

	if *applicationRole == "" || *applicationRole == "test" {
		test1()
	} else if *applicationRole == "master" {
		node.StartMasterNode()
	} else if *applicationRole == "slave" {
		node.StartSlaveNode(*masterNodeHost, *masterNodePort, *nodeIdDirectory)
	} else {
		stdlog.Fatalf("parameter role '%v' is not recognized", *applicationRole)
	}

}

func test1() {
	config, err := handling.ReadConfig()
	if err != nil {
		stdlog.Fatal(err)
	}
	fmt.Println(config)

	_, err = repository.InitRepository(hearth.AdaptRepositoryConfig(config))
	if err != nil {
		stdlog.Fatal(err)
	}

	if err != nil {
		stdlog.Fatal(err)
	}

	ctx := context.Background()
	table, err := repository.TableInfoById(ctx, 2) //100:111
	if err != nil {
		stdlog.Fatal(err)
	}
	if true {
		l := logrus.New()
		l = logrus.StandardLogger()
		_ = l
		indexer, err := index.NewIndexer(
			&index.BitsetIndexConfigType{
				DumperConfig: hearth.AdaptDataReaderConfig(config),
				BitsetPath:   config.BitsetPath,
				Log: l,
			},
		)
		err = indexer.BuildBitsets(
			ctx,
			dto.NewBitSetContents(dto.HashContent),
			config.AstraDumpPath,
			table,
		)

		if err != nil {
			stdlog.Fatal(err)
		}
	}
	if false {
		splitter, err := categorysplit.NewCategorySplitter(&categorysplit.ConfigType{
			DumpReaderConfig:     hearth.AdaptDataReaderConfig(config),
			PathToSliceDirectory: config.BitsetPath,
			MaxRowCountPerFile:   1000000,

		})
		if err != nil {
			err = fmt.Errorf("could not do category split for table %v: %v", table, err)
			stdlog.Fatal(err)
		}
		err = splitter.SplitFile(
			ctx,
			path.Join(config.AstraDumpPath, table.PathToFile.String()),
			dto.ColumnInfoListType{table.Columns[1],table.Columns[2],table.Columns[3]}, //7:2
		)
		if err != nil {
			stdlog.Fatal(err)
		}
	}
	if false {
		dumpConfig := hearth.AdaptDataReaderConfig(config)
		dumpConfig.GZip = false
		sorter := sort.NewColumnSorter(&sort.ColumnSorterConfigType{
			PathToSortedSliceDirectory: "./Sorted",
			DumpReaderConfig:           dumpConfig,
		})

		err = sorter.SortByColumn(ctx,
			"C:/home/vlad/data.253.4/BINDATA/111/175692/175807/144764187",
			1000,
			misc.PositionFlagsAs(misc.PositionOn, len(table.ColumnList()), []int{8}...),
		)
		if err != nil {
			stdlog.Fatal(err)
		}

		searcher, _ := search.NewColumnSearcher(
			&search.ColumnSearcherConfigType{
				DumpReaderConfig: *dumpConfig,
			})

		sortedFile := "C:/home/vlad/data.253.4/BINDATA/111/175692/175807/144764187.sorted"
		s1, s2, s3 := searcher.Search(ctx, sortedFile, true, 8, 8, [][]byte{
			[]byte("121185"),
			[]byte("999-83-7009"),
			[]byte("35183"),
			[]byte("1997-09-10 00:00:00.0"),
			[]byte("2003.8231.8275.4524"),
			[]byte("LONG TERM"),
			[]byte("1293200"),
			[]byte("USD"),
			[]byte("1998-05-25 00:00:00.0"),
			[]byte("LAND"),
			[]byte("6324500"),
			[]byte("USD"),
		},
		)
		fmt.Println(s1)
		fmt.Println(s2)
		fmt.Println(s3)
	}
}
