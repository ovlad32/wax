package main

import (
	"context"
	"flag"
	"github.com/goinggo/tracelog"
	"github.com/ovlad32/wax/hearth"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling"
	index "github.com/ovlad32/wax/hearth/process/index"
	"github.com/ovlad32/wax/hearth/repository"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"github.com/ovlad32/wax/hearth/process/categorysplit"
	"path"
)

var packageName = "main"

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

var pathToConfigFile = flag.String("configfile", "./config.json", "path to config file")
var argMetadataIds = flag.String("metadata_id", string(-math.MaxInt64), "")
var argWorkflowIds = flag.String("workflow_id", string(-math.MaxInt64), "")

func main() {
	flag.Parse()
	tracelog.Start(tracelog.LevelInfo)
	defer tracelog.Stop()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		defer f.Close()
	}

	test1()

}

func test1() {

	config, err := handling.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}

	err = repository.InitRepository(hearth.AdaptRepositoryConfig(config))
	if err != nil {
		log.Fatal(err)
	}

	indexer, err := index.NewIndexer(
		&index.BitsetIndexConfigType{
			DumperConfig: *hearth.AdaptDataReaderConfig(config),
			BitsetPath:       config.BitsetPath,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	table, err := repository.TableInfoById(ctx, 111)
	if err != nil {
		log.Fatal(err)
	}
	_ = indexer
	/*
	err = index.BuildBitsets(
		ctx,
		dto.NewBitSetContents(dto.HashContent),
		table,
	)

	if err != nil {
		log.Fatal(err)
	}*/

	splitter,err  :=categorysplit.NewCategorySpliter(&categorysplit.CategorySplitConfigType{
		DumpReaderConfig:*hearth.AdaptDataReaderConfig(config),
		PathToSliceDirectory: config.BitsetPath,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = splitter.SplitFile(
		ctx,
		path.Join(config.AstraDumpPath,table.PathToFile.String()),
		dto.ColumnInfoListType{table.Columns[7]},
	)
	if err != nil {
		log.Fatal(err)
	}

}
