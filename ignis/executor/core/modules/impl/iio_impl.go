package impl

import "C"
import (
	"bufio"
	"bytes"
	"errors"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	iio "ignis/executor/core/iio"
	"ignis/executor/core/impi"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/executor/core/utils"
	"io"
	"io/fs"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"unsafe"
	"github.com/andreasolla/hdfs/v2"
)

type IIOImpl struct {
	IBaseImpl
}

func NewIIOImpl(executorData *core.IExecutorData) *IIOImpl {
	return &IIOImpl{
		IBaseImpl{executorData},
	}
}

func PartitionApproxSize[T any](this *IIOImpl) (int64, error) {
	logger.Info("IO: calculating partition size")
	group, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return 0, ierror.Raise(err)
	}
	if group.Size() == 0 {
		return 0, nil
	}

	size := int64(0)
	if this.executorData.GetPartitionTools().IsMemoryGroup(group) {
		for _, part := range group.Iter() {
			size += part.Size()
		}
		if iio.IsContiguous[T]() {
			size *= int64(utils.TypeObj[T]().Size())
		} else {
			if eSize, err := this.executorData.GetProperties().TransportElemSize(); err != nil {
				return 0, ierror.Raise(err)
			} else {
				size *= eSize
			}
		}
	} else {
		for _, part := range group.Iter() {
			size += part.Bytes()
		}
	}

	return size, nil
}

func (this *IIOImpl) PlainFile(path string, minPartitions int64, delim string) error {
	logger.Info("IO: reading plain file")
	return this.plainOrTextFile(path, minPartitions, delim)
}

func (this *IIOImpl) TextFile(path string, minPartitions int64) error {
	if strings.HasPrefix(path, "hdfs://") {

		return this.textFileHDFS(path, minPartitions)
	}
	logger.Info("IO: reading text file")
	return this.plainOrTextFile(path, minPartitions, "\n")
}

func readBytes(reader *bufio.Reader, buffer *[]byte, delim []byte, exs [][]byte) ([]byte, error) {
	if len(delim) == 1 && len(exs) == 0 {
		return reader.ReadBytes(delim[0])
	}
	*buffer = (*buffer)[:0]
	var line []byte
	var err error
	dsize := len(delim)
	overlap := 0
OUT:
	for err != io.EOF {
		line, err = reader.ReadBytes(delim[dsize-1])
		if err != nil && err != io.EOF {
			return nil, ierror.Raise(err)
		}
		*buffer = append(*buffer, line...)
		if dsize == 1 || (len(*buffer) >= dsize && bytes.Compare((*buffer)[len(*buffer)-dsize:], delim) == 0) {
			if len(exs) > 0 {
				if len(*buffer)-overlap < dsize {
					continue OUT
				}
				for _, ex := range exs {
					if len(*buffer) > len(ex) && bytes.Compare((*buffer)[len(*buffer)-len(ex):], ex) == 0 {
						overlap = len(*buffer)
						continue OUT
					}
				}
			}
			break
		}
	}
	return (*buffer)[:], err
}

func (this *IIOImpl) plainOrTextFile(path string, minPartitions int64, delim string) error {
	size := int64(0)
	if info, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return ierror.Raise(err)
	} else {
		size = info.Size()
	}
	logger.Info("IO: file has ", size, " Bytes")
	result, err := core.NewPartitionGroupDef[string](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}
	threadGroup := make([]*storage.IPartitionGroup[string], ioCores)
	elements := int64(0)

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		file, err := this.openFileRead(path)
		if err != nil {
			return ierror.Raise(err)
		}
		defer file.Close()
		id := rctx.ThreadId()
		globalThreadId := this.executorData.GetContext().ExecutorId()*ioCores + id
		threads := this.executorData.GetContext().Executors() * ioCores
		exChunk := size / int64(threads)
		exChunkInit := int64(globalThreadId) * exChunk
		exChunkEnd := exChunkInit + exChunk
		minPartitionSize, err := this.executorData.GetProperties().PartitionMinimal()
		if err != nil {
			return ierror.Raise(err)
		}
		minPartitions := int64(math.Ceil(float64(minPartitions) / float64(threads)))
		ldelim := delim
		buffer := make([]byte, 0, 1024)
		exs := make([][]byte, 0)
		esize := 0
		if strings.ContainsRune(ldelim, '!') {
			flag := rune(0)
			for ; strings.ContainsRune(ldelim, flag); flag++ {
			}
			ldelim = strings.ReplaceAll(ldelim, "\\!", string(flag))
			fields := strings.Split(ldelim, "!")
			for i, _ := range fields {
				fields[i] = strings.ReplaceAll(fields[i], string(flag), "!")
				if i == 0 {
					ldelim = fields[0]
				} else {
					exs = append(exs, []byte(fields[i]+ldelim))
				}
			}
			for _, ex := range exs {
				if len(ex) > esize {
					esize = len(ex) + 1
				}
			}
		}
		if len(ldelim) == 0 {
			ldelim = "\n"
		}
		dsize := len(ldelim)
		bdelim := []byte(ldelim)

		if globalThreadId > 0 {
			padding := utils.Ternary(exChunkInit >= int64(dsize+esize), exChunkInit-int64(dsize+esize), 0)
			if _, err = file.Seek(padding, 0); err != nil {
				return ierror.Raise(err)
			}

			reader := bufio.NewReader(file)
			for true {
				if chunk, err := readBytes(reader, &buffer, bdelim, exs); err != nil && err != io.EOF {
					return ierror.Raise(err)
				} else {
					padding += int64(len(chunk))
				}
				if exChunkInit <= padding {
					break
				}
			}
			exChunkInit = padding

			if _, err = file.Seek(exChunkInit, 0); err != nil {
				return ierror.Raise(err)
			}
			if globalThreadId == threads-1 {
				exChunkEnd = size
			}
		}

		if exChunk/minPartitionSize < minPartitions {
			minPartitionSize = exChunk / minPartitions
		}

		if threadGroup[id], err = core.NewPartitionGroupDef[string](this.executorData.GetPartitionTools()); err != nil {
			return ierror.Raise(err)
		}
		partition, err := core.NewPartitionDef[string](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		writeIterator, err := partition.WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		threadGroup[id].Add(partition)
		threadElements := int64(0)
		partitionInit := exChunkInit
		filepos := exChunkInit
		reader := bufio.NewReaderSize(file, utils.Min(1*10e9, utils.Max(65*1024, int(minPartitionSize/(4*int64(ioCores))))))
		for filepos < exChunkEnd {
			if (filepos - partitionInit) > minPartitionSize {
				if err = partition.Fit(); err != nil {
					return ierror.Raise(err)
				}
				if partition, err = core.NewPartitionDef[string](this.executorData.GetPartitionTools()); err != nil {
					return ierror.Raise(err)
				}
				if writeIterator, err = partition.WriteIterator(); err != nil {
					return ierror.Raise(err)
				}
				threadGroup[id].Add(partition)
				partitionInit = filepos
			}
			line, err := readBytes(reader, &buffer, bdelim, exs)
			eof := err == io.EOF
			if err != nil && err != io.EOF {
				return ierror.Raise(err)
			}
			filepos += int64(len(line))
			threadElements++
			if eof {
				if err := writeIterator.Write(string(line)); err != nil {
					return ierror.Raise(err)
				}
				break
			} else {
				if err := writeIterator.Write(string(line[:len(line)-dsize])); err != nil {
					return ierror.Raise(err)
				}
			}
		}
		return rctx.Critical(func() error {
			elements += threadElements
			return nil
		})
	}); err != nil {
		return err
	}

	for _, group := range threadGroup {
		for _, part := range group.Iter() {
			result.Add(part)
		}
	}

	logger.Info("IO: created ", result.Size(), " partitions, ", elements, " lines and ", size, " Bytes read")
	core.SetPartitions[string](this.executorData, result)
	return nil
}

func (this *IIOImpl) textFileHDFS(path string, minPartitions int64) error {
	order, _ := this.executorData.GetProperties().PreserveOrder()

	if order {
		return this.hdfsNotOrdering(path)
	}

	hostPath, err := this.executorData.GetProperties().GetHdfsPath()
	if err != nil {
		return ierror.Raise(err)
	}

	client, err := hdfs.New(hostPath)
	if err != nil {
		return ierror.Raise(err)
	}

	filepath := path[len("hdfs:/"):]

	size := int64(0)
	if info, err := client.Stat(filepath); err != nil {
		return ierror.Raise(err)
	} else {
		size = info.Size()
	}
	logger.Info("IO: file has ", size, " Bytes")
	result, err := core.NewPartitionGroupDef[string](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}
	threadGroup := make([]*storage.IPartitionGroup[string], ioCores)
	elements := int64(0)

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		file, err := client.Open(filepath)
		if err != nil {
			return ierror.Raise(err)
		}
		defer file.Close()
		id := rctx.ThreadId()
		globalThreadId := this.executorData.GetContext().ExecutorId()*ioCores + id
		threads := this.executorData.GetContext().Executors() * ioCores
		exChunk := size / int64(threads)
		exChunkInit := int64(globalThreadId) * exChunk
		exChunkEnd := exChunkInit + exChunk
		minPartitionSize, err := this.executorData.GetProperties().PartitionMinimal()
		if err != nil {
			return ierror.Raise(err)
		}
		minPartitions := int64(math.Ceil(float64(minPartitions) / float64(threads)))
		ldelim := "\n"
		buffer := make([]byte, 0, 1024)
		exs := make([][]byte, 0)

		dsize := len(ldelim)
		bdelim := []byte(ldelim)

		if globalThreadId > 0 {
			padding := utils.Ternary(exChunkInit >= int64(dsize), exChunkInit-int64(dsize), 0)
			if _, err = file.Seek(padding, 0); err != nil {
				return ierror.Raise(err)
			}

			reader := bufio.NewReader(file)
			for true {
				if chunk, err := readBytes(reader, &buffer, bdelim, exs); err != nil && err != io.EOF {
					return ierror.Raise(err)
				} else {
					padding += int64(len(chunk))
				}
				if exChunkInit <= padding {
					break
				}
			}
			exChunkInit = padding

			if _, err = file.Seek(exChunkInit, 0); err != nil {
				return ierror.Raise(err)
			}
			if globalThreadId == threads-1 {
				exChunkEnd = size
			}
		}

		if exChunk/minPartitionSize < minPartitions {
			minPartitionSize = exChunk / minPartitions
		}

		if threadGroup[id], err = core.NewPartitionGroupDef[string](this.executorData.GetPartitionTools()); err != nil {
			return ierror.Raise(err)
		}
		partition, err := core.NewPartitionDef[string](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		writeIterator, err := partition.WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		threadGroup[id].Add(partition)
		threadElements := int64(0)
		partitionInit := exChunkInit
		filepos := exChunkInit
		reader := bufio.NewReaderSize(file, utils.Min(1*10e9, utils.Max(65*1024, int(minPartitionSize/(4*int64(ioCores))))))
		for filepos < exChunkEnd {
			if (filepos - partitionInit) > minPartitionSize {
				if err = partition.Fit(); err != nil {
					return ierror.Raise(err)
				}
				if partition, err = core.NewPartitionDef[string](this.executorData.GetPartitionTools()); err != nil {
					return ierror.Raise(err)
				}
				if writeIterator, err = partition.WriteIterator(); err != nil {
					return ierror.Raise(err)
				}
				threadGroup[id].Add(partition)
				partitionInit = filepos
			}
			line, err := readBytes(reader, &buffer, bdelim, exs)
			eof := err == io.EOF
			if err != nil && err != io.EOF {
				return ierror.Raise(err)
			}
			filepos += int64(len(line))
			threadElements++
			if eof {
				if err := writeIterator.Write(string(line)); err != nil {
					return ierror.Raise(err)
				}
				break
			} else {
				if err := writeIterator.Write(string(line[:len(line)-dsize])); err != nil {
					return ierror.Raise(err)
				}
			}
		}
		return rctx.Critical(func() error {
			elements += threadElements
			return nil
		})
	}); err != nil {
		return err
	}

	for _, group := range threadGroup {
		for _, part := range group.Iter() {
			result.Add(part)
		}
	}

	logger.Info("IO: created ", result.Size(), " partitions, ", elements, " lines and ", size, " Bytes read")
	core.SetPartitions[string](this.executorData, result)
	return nil
}

type ByExecutorID []interface{}

func (a ByExecutorID) Len() int      { return len(a) }
func (a ByExecutorID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByExecutorID) Less(i, j int) bool {
	return a[i].([]interface{})[0].(string) < a[j].([]interface{})[0].(string)
}

func sorted(data []interface{}, less func(i, j int) bool) []interface{} {
	sortedData := make([]interface{}, len(data))
	copy(sortedData, data)
	sort.Slice(sortedData, less)
	return sortedData
}

func (this *IIOImpl) assignedBlocks(blocks []hdfs.BlockInfo) map[uint64]hdfs.BlockInfo {
	logger.Info("IO: distributing blocks")
	executorId := this.executorData.GetContext().ExecutorId() 
	executors := this.executorData.GetContext().Executors()
	executorIP := this.executorData.GetContext().Props()["ignis.executor.host"]
	blocksPerExecutor := int(math.Ceil(float64(len(blocks)) / float64(executors)))

	executorBlocks := make([]hdfs.BlockInfo, 0)
	notAssignedBlocks := make(map[uint64]hdfs.BlockInfo)
	for i := 0; i < len(blocks); i++ {
		notAssignedBlocks[blocks[i].GetBlockId()] = blocks[i]
		if blocks[i].GetIpAddr()[0] == executorIP {
			executorBlocks = append(executorBlocks, blocks[i])
		}
	}

	//------------------------------
	// Obtengo el orden de los ejecutores
	executorsOrder := make([]int, executors)
	impi.MPI_Allgather(unsafe.Pointer(&executorId), 1, impi.MPI_INT, unsafe.Pointer(&executorsOrder[0]), 1, impi.MPI_INT, this.executorData.GetContext().MpiGroup())

	//Obtengo el numero de bloques de cada ejecutor
	executorsNBlocks := make([]C.int, executors)
	numBlocks := len(executorBlocks)
	impi.MPI_Allgather(unsafe.Pointer(&numBlocks), 1, impi.MPI_INT, unsafe.Pointer(&executorsNBlocks[0]), 1, impi.MPI_INT, this.executorData.GetContext().MpiGroup())

	// Paso los BlockID de executorBlocks a un array de bloques
	executorBlocksArray := make([]int, len(executorBlocks))
	for i := 0; i < len(executorBlocks); i++ {
		executorBlocksArray[i] = int(executorBlocks[i].GetBlockId())
	}

	// Array de posiciones donde se guardaran los bloques de cada ejecutor
	executorsBlocksPos := make([]C.int, executors)
	executorsBlocksPos[0] = 0
	for i := 1; i < executors; i++ {
		executorsBlocksPos[i] = executorsBlocksPos[i-1] + executorsNBlocks[i-1]
	}

	//Obtengo el numero total de bloques
	totalBlocks := 0
	for i := 0; i < executors; i++ {
		totalBlocks += int(executorsNBlocks[i])
	}
	// Obtengo todos los bloques de todos los ejecutores con allgatherv
	allBlocksArray := make([]int, totalBlocks)
	aux := len(executorBlocks)
	impi.MPI_Allgatherv(unsafe.Pointer(&executorBlocksArray[0]), impi.C_int(C.int(aux)), impi.MPI_INT, unsafe.Pointer(&allBlocksArray[0]), (*impi.C_int)(&executorsNBlocks[0]), (*impi.C_int)(&executorsBlocksPos[0]), impi.MPI_INT, this.executorData.GetContext().MpiGroup())

	allBlocks := make([]interface{}, 0)

	for i := 0; i < executors; i++ {
		blockInfos := make([]int, 0)
		for j := 0; j < int(executorsNBlocks[i]); j++ {
			blockInfos = append(blockInfos, allBlocksArray[int(executorsBlocksPos[i])+j])
		}
		pair := []interface{}{executorsOrder[i], blockInfos}
		allBlocks = append(allBlocks, pair)
	}

	//------------------------------

	sortedData := sorted(allBlocks, func(i, j int) bool {
		return allBlocks[i].([]interface{})[0].(int) < allBlocks[j].([]interface{})[0].(int)
	})

	// Reparto de bloques por IP
	takenBlocks := make([]uint64, 0)
	assignedBlocks := make(map[uint64]hdfs.BlockInfo)
	numberBlocks := make([]int, executors)
	for i := 0; i < executors; i++ {
		taken := 0
		pos := 0
		nBlocks := 0
		for len(sortedData[i].([]interface{})[1].([]hdfs.BlockInfo)) > 0 && taken < blocksPerExecutor && pos < len(sortedData[i].([]interface{})[1].([]hdfs.BlockInfo)) {
			if _, exists := contains(takenBlocks, sortedData[i].([]interface{})[1].([]hdfs.BlockInfo)[pos].GetBlockId()); !exists {
				takenBlocks = append(takenBlocks, sortedData[i].([]interface{})[1].([]hdfs.BlockInfo)[pos].GetBlockId())
				taken++
				nBlocks++
				if sortedData[i].([]interface{})[0].(int) == executorId {
					assignedBlocks[sortedData[i].([]interface{})[1].([]hdfs.BlockInfo)[pos].GetBlockId()] = sortedData[i].([]interface{})[1].([]hdfs.BlockInfo)[pos]
				}
			}
			pos++
		}
		numberBlocks[i] = nBlocks
	}

	// Bloques que no han sido asignados
	lostBlocks := make([]hdfs.BlockInfo, 0)
	for i := 0; i < len(blocks); i++ {
		if _, exists := contains(takenBlocks, blocks[i].GetBlockId()); !exists {
			lostBlocks = append(lostBlocks, blocks[i])
		}
	}

	// Si aun no se han asignado todos los bloques, se asignan los que faltan
	if len(assignedBlocks) < blocksPerExecutor && len(lostBlocks) > 0 {
		executor := 0
		i := 0
		for executor < executorId && len(lostBlocks) > i {
			i += blocksPerExecutor - numberBlocks[executor]
			executor++
		}

		for len(assignedBlocks) < blocksPerExecutor && len(lostBlocks) > i {
			assignedBlocks[lostBlocks[i].GetBlockId()] = lostBlocks[i]
			i++
		}
	}

	return assignedBlocks
}

func contains(arr []uint64, str uint64) (int, bool) {
	for i, a := range arr {
		if a == str {
			return i, true
		}
	}
	return -1, false
}

func (this *IIOImpl) hdfsNotOrdering(path string) error {
	hostPath, err := this.executorData.GetProperties().GetHdfsPath()
	if err != nil {
		return ierror.Raise(err)
	}

	client, err := hdfs.New(hostPath)
	if err != nil {
		return ierror.Raise(err)
	}

	filepath := path[len("hdfs:/"):]

	size := int64(0)
	if info, err := client.Stat(filepath); err != nil {
		return ierror.Raise(err)
	} else {
		size = info.Size()
	}
	logger.Info("IO: file has ", size, " Bytes")

	result, err := core.NewPartitionGroupDef[string](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}
	threadGroup := make([]*storage.IPartitionGroup[string], ioCores)
	elements := int64(0)

	//Reparto de bloques
	file, err := client.Open(filepath)
	if err != nil {
		return ierror.Raise(err)
	}
	blocks, err := file.GetBlocks()
	if err != nil {
		return ierror.Raise(err)
	}
	file.Close()
	blocksToRead := this.assignedBlocks(blocks)

	//Obtencion de los bloques a leer
	first := blocks[0].GetBlockId()
	blockSize := int64(blocks[0].GetNumBytes())
	myBlocks := make([]hdfs.BlockInfo, 0)

	for i := 0; i < len(blocks); i++ {
		if _, ok := blocksToRead[blocks[i].GetBlockId()]; ok {
			myBlocks = append(myBlocks, blocks[i])
		}
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		file, err := client.Open(filepath)
		if err != nil {
			return ierror.Raise(err)
		}
		defer file.Close()

		id := rctx.ThreadId()
		globalThreadId := this.executorData.GetContext().ExecutorId()*ioCores + id
		threads := this.executorData.GetContext().Executors() * ioCores
		numBlocks := int(math.Ceil(float64(len(myBlocks) / threads)))

		// lectura
		threadElements := int64(0)
		for i := id * numBlocks; i < (id+1)*numBlocks; i++ {
			exChunkInit := int64((myBlocks[i].GetBlockId() - first)) * blockSize
			exChunkEnd := exChunkInit + int64(myBlocks[i].GetNumBytes())
			ldelim := "\n"
			buffer := make([]byte, 0, 1024)
			exs := make([][]byte, 0)

			dsize := len(ldelim)
			bdelim := []byte(ldelim)

			if myBlocks[i].GetBlockId() == first {
				padding := utils.Ternary(exChunkInit >= int64(dsize), exChunkInit-int64(dsize), 0)
				if _, err = file.Seek(padding, 0); err != nil {
					return ierror.Raise(err)
				}

				reader := bufio.NewReader(file)
				for true {
					if chunk, err := readBytes(reader, &buffer, bdelim, exs); err != nil && err != io.EOF {
						return ierror.Raise(err)
					} else {
						padding += int64(len(chunk))
					}
					if exChunkInit <= padding {
						break
					}
				}
				exChunkInit = padding

				if _, err = file.Seek(exChunkInit, 0); err != nil {
					return ierror.Raise(err)
				}
				if globalThreadId == threads-1 {
					exChunkEnd = size
				}
			}

			if threadGroup[id], err = core.NewPartitionGroupDef[string](this.executorData.GetPartitionTools()); err != nil {
				return ierror.Raise(err)
			}
			partition, err := core.NewPartitionDef[string](this.executorData.GetPartitionTools())
			if err != nil {
				return ierror.Raise(err)
			}
			writeIterator, err := partition.WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			threadGroup[id].Add(partition)
			partitionInit := exChunkInit
			filepos := exChunkInit
			reader := bufio.NewReaderSize(file, utils.Min(1*10e9, utils.Max(65*1024, int(blockSize/(4*int64(ioCores))))))
			for filepos < exChunkEnd {
				if (filepos - partitionInit) > blockSize {
					if err = partition.Fit(); err != nil {
						return ierror.Raise(err)
					}
					if partition, err = core.NewPartitionDef[string](this.executorData.GetPartitionTools()); err != nil {
						return ierror.Raise(err)
					}
					if writeIterator, err = partition.WriteIterator(); err != nil {
						return ierror.Raise(err)
					}
					threadGroup[id].Add(partition)
					partitionInit = filepos
				}
				line, err := readBytes(reader, &buffer, bdelim, exs)
				eof := err == io.EOF
				if err != nil && err != io.EOF {
					return ierror.Raise(err)
				}
				filepos += int64(len(line))
				threadElements++
				if eof {
					if err := writeIterator.Write(string(line)); err != nil {
						return ierror.Raise(err)
					}
					break
				} else {
					if err := writeIterator.Write(string(line[:len(line)-dsize])); err != nil {
						return ierror.Raise(err)
					}
				}
			}
		}
		return rctx.Critical(func() error {
			elements += threadElements
			return nil
		})

	}); err != nil {
		return err
	}

	for _, group := range threadGroup {
		for _, part := range group.Iter() {
			result.Add(part)
		}
	}

	logger.Info("IO: created ", result.Size(), " partitions, ", elements, " lines and ", size, " Bytes read")
	core.SetPartitions[string](this.executorData, result)
	return nil
}

func PartitionObjectFile[T any](this *IIOImpl, path string, first int64, partitions int64) error {
	logger.Info("IO: reading partition object file")
	group, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(int(partitions), func(p int) error {
			fileName, err := this.partitionFileName(path, first+int64(p))
			if err != nil {
				return ierror.Raise(err)
			}
			file, err := this.openFileRead(fileName) //Only to check
			if err != nil {
				return ierror.Raise(err)
			}
			_ = file.Close()
			open, err := storage.NewIDiskPartition[T](fileName, 0, true, true, true)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = group.Get(p).CopyFrom(open); err != nil {
				return ierror.Raise(err)
			}
			return ierror.Raise(group.Get(p).Fit())
		})
	}); err != nil {
		return err
	}

	core.SetPartitions[T](this.executorData, group)
	return nil
}

func (this *IIOImpl) PartitionTextFile(path string, first int64, partitions int64) error {
	logger.Info("IO; reading partitions text file")
	group, err := core.NewPartitionGroupWithSize[string](this.executorData.GetPartitionTools(), int(partitions))
	if err != nil {
		return ierror.Raise(err)
	}

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(int(partitions), func(p int) error {
			fileName, err := this.partitionFileName(path, first+int64(p))
			if err != nil {
				return ierror.Raise(err)
			}
			file, err := this.openFileRead(fileName)
			if err != nil {
				return ierror.Raise(err)
			}
			partition := group.Get(p)
			writeIterator, err := partition.WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			reader := bufio.NewReader(file)
			for true {
				line, err := reader.ReadBytes('\n')
				eof := err == io.EOF
				if err != nil && err != io.EOF {
					return ierror.Raise(err)
				}
				if eof {
					if len(line) == 0 {
						break
					}
					if err = writeIterator.Write(string(line)); err != nil {
						return ierror.Raise(err)
					}
					return nil
				} else {
					if err = writeIterator.Write(string(line[:len(line)-1])); err != nil {
						return ierror.Raise(err)
					}
				}
			}
			return nil
		})
	}); err != nil {
		return err
	}

	core.SetPartitions[string](this.executorData, group)
	return nil
}

func PartitionJsonFile[T any](this *IIOImpl, path string, first int64, partitions int64) error {
	logger.Info("IO: reading partition json file")
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func SaveAsObjectFile[T any](this *IIOImpl, path string, compression int8, first int64) error {
	logger.Info("IO: saving as object file")
	group, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			var fileName string
			if err := rctx.Critical(func() error {
				fileName, err = this.partitionFileName(path, first+int64(p))
				if err != nil {
					return ierror.Raise(err)
				}
				file, err := this.openFileWrite(fileName) //Only to check
				if err != nil {
					return ierror.Raise(err)
				}
				_ = file.Close()
				return err
			}); err != nil {
				return ierror.Raise(err)
			}

			save, err := storage.NewIDiskPartition[T](fileName, 0, true, true, false)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = group.Get(p).CopyTo(save); err != nil {
				return ierror.Raise(err)
			}
			if err = save.Sync(); err != nil {
				return ierror.Raise(err)
			}
			group.SetBase(p, nil)
			return nil
		})
	}); err != nil {
		return err
	}

	return nil
}

func SaveAsTextFile[T any](this *IIOImpl, path string, first int64) error {
	if strings.HasPrefix(path, "hdfs://") {

		return SaveAsHdfsFile[T](this, path, first)
	}
	logger.Info("IO: saving as text file")
	group, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	isMemory := this.executorData.GetPartitionTools().IsMemoryGroup(group)

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			var fileName string
			var file *os.File
			if err := rctx.Critical(func() error {
				fileName, err = this.partitionFileName(path, first+int64(p))
				if err != nil {
					return ierror.Raise(err)
				}
				file, err = this.openFileWrite(fileName)
				if err != nil {
					return ierror.Raise(err)
				}
				return err
			}); err != nil {
				return ierror.Raise(err)
			}
			defer file.Close()

			if isMemory {
				list := group.Get(p).Inner().(storage.IList)
				if err = iio.Print(file, list.Array()); err != nil {
					return ierror.Raise(err)
				}
			} else {
				it, err := group.Get(p).ReadIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = iio.Print(file, it); err != nil {
					return ierror.Raise(err)
				}
			}

			group.SetBase(p, nil)
			return nil
		})
	}); err != nil {
		return err
	}

	return nil
}

func SaveAsHdfsFile[T any](this *IIOImpl, path string, first int64) error {
	logger.Info("IO: saving as HDFS file")

	hostPath, err := this.executorData.GetProperties().GetHdfsPath()
	if err != nil {
		return ierror.Raise(err)
	}

	client, err := hdfs.New(hostPath)
	if err != nil {
		return ierror.Raise(err)
	}

	filepath := path[len("hdfs:/"):]

	group, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	isMemory := this.executorData.GetPartitionTools().IsMemoryGroup(group)

	ioCores, err := this.ioCores()
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.ParallelT(ioCores, func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			var fileName string
			var file *hdfs.FileWriter
			if err := rctx.Critical(func() error {
				fileName, err = this.partitionFileName(filepath, first+int64(p))
				if err != nil {
					return ierror.Raise(err)
				}
				dir := strings.Split(fileName, "/")
				dir = dir[:len(dir)-1]
				dirpath := strings.Join(dir, "/") + "/"
				err := client.MkdirAll(dirpath, 0777)
				if err != nil {
					return ierror.Raise(err)
				}
				file, err = client.Create(fileName)
				if err != nil {
					return ierror.Raise(err)
				}
				return err
			}); err != nil {
				return ierror.Raise(err)
			}
			defer file.Close()

			if isMemory {
				list := group.Get(p).Inner().(storage.IList)
				if err = iio.Print(file, list.Array()); err != nil {
					return ierror.Raise(err)
				}
			} else {
				it, err := group.Get(p).ReadIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = iio.Print(file, it); err != nil {
					return ierror.Raise(err)
				}
			}

			group.SetBase(p, nil)
			return nil
		})
	}); err != nil {
		return err
	}

	return nil
}

func SaveAsJsonFile[T any](this *IIOImpl, path string, first int64, pretty bool) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IIOImpl) partitionFileName(path string, index int64) (string, error) {
	if info, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return "", ierror.RaiseMsgCause("Unable to create directory "+path, err)
		}
	} else if errors.Is(err, fs.ErrExist) && !info.IsDir() {
		return "", ierror.RaiseMsg("Unable to create directory " + path)
	}

	strIndex := strconv.FormatInt(index, 10)
	zeros := utils.Max(6-len(strIndex), 0)
	return path + "/part" + strings.Repeat("0", zeros) + strIndex, nil
}

func (this *IIOImpl) openFileRead(path string) (*os.File, error) {
	logger.Info("IO: opening file ", path)
	if info, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return nil, ierror.RaiseMsgCause(path+" was not found", err)
	} else if !info.Mode().IsRegular() {
		return nil, ierror.RaiseMsg(path + " was not a file")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, ierror.RaiseMsgCause(path+" cannot be opened", err)
	}
	logger.Info("IO: file opening successful")
	return file, nil
}

func (this *IIOImpl) openFileWrite(path string) (*os.File, error) {
	logger.Info("IO: creating file ", path)
	if _, err := os.Stat(path); errors.Is(err, fs.ErrExist) {
		if o, err := this.executorData.GetProperties().IoOverwrite(); err != nil {
			return nil, ierror.Raise(err)
		} else {
			if o {
				logger.Warn("IO: ", path, " already exists")
				if err = os.Remove(path); err != nil {
					return nil, ierror.RaiseMsgCause(path+" can not be removed", err)
				}
			} else {
				return nil, ierror.RaiseMsg(path + " already exists")
			}
		}
		return nil, ierror.RaiseMsgCause(path+" was not found", err)
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, ierror.RaiseMsgCause(path+" cannot be opened", err)
	}
	logger.Info("IO: file created successful")
	return file, nil
}

func (this *IIOImpl) ioCores() (int, error) {
	cores, err := this.executorData.GetProperties().IoCores()
	if err != nil {
		return 1, ierror.Raise(err)
	}
	if cores > 1 {
		return utils.Min(this.executorData.GetCores(), int(math.Ceil(cores))), nil
	}
	return utils.Max(1, int(math.Ceil(cores*float64(this.executorData.GetCores())))), nil
}
