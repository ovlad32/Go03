package main

/*
func main2() {
	hs = fnv.New64()
	//match = 0
	//Zip()
	//Unzip()
	/*
	buff := []byte{1,2,10,12,11,10,10,10,22,75,10,5}
	fmt.Println(buff)
	r:= bytes.Split(buff,[]byte{10})
	fmt.Println(r)
	for n ,_ := range r {
		fmt.Println(n)
	}
	return*/
//Sparse1 := buildBitSet(1,"c:/home/cra.liabilities.txt", 7,13)
//Sparse3 := buildBitSet(2,"c:/home/mt.contracts.txt", 9 ,15)
//Sparse1 := buildBitSet(1,"c:/home/cra.liabilities.txt", 2,13)
//Sparse3 := buildBitSet(2,"c:/home/mt.contracts.txt", 0 ,15)
/*Sparse1 := buildBitSet(1,"c:/home/cra.liabilities.txt", 3,13)
Sparse3 := buildBitSet(2,"c:/home/mt.contracts.txt", 6 ,15)
fmt.Println((*Sparse1).Len(),(*Sparse3).Len())
diff,_ := (*Sparse3).IntersectionCardinality(Sparse1)
fmt.Println((*Sparse1).Cardinality(),(*Sparse3).Cardinality(),diff)
Result := (*Sparse1).Intersection(Sparse3)
//fmt.Println((*Result).Cardinality())
osReader,_ := os.Open("c:/home/cra.liabilities.txt")
datFile,_ := bufio.NewReader(osReader)
readers := make(map[string]io.Reader)
for pos, e :=Result.NextSet(0);e; pos,e = Result.NextSet(pos+1){
	key := getHLKey(pos)
	var reader io.Reader
	if  foundReader,found := readers[key];!found {
		reader,_ = os.Open("c:/home/HL/1/3/"+key)
		readers[key] = reader
	} else {
		reader = foundReader
	}
	len := 8+4+8
	for ;;{
		buffer := make([]byte,len)
		reader.Read(buffer)
		hf := (*uint64)(unsafe.Pointer(&buffer[0]))
		if *hf == pos {
			datPos := (*uint64)(unsafe.Pointer(&buffer[6]))
			datFile.Reset();
			datFile.Discard(datPos)
		}

	}


}
//	fmt.Println(Sparse1.Test(uint64(match)),Sparse3.Test(uint64(match)))
/*var sparse1Count,sparse3Count,intersection int64 = 0,0,0
l:= len(Sparse1);
if l>len(Sparse3) {
	l=len(Sparse3)
}

for i:=0;i<math.MaxInt64;i++{
	sp1,sp3 := Sparse1.Get(i),Sparse3.Get(i)
	if sp1 {sparse1Count++}
	if sp3 {sparse3Count++}
	if  sp1 && sp3  { intersection ++

	}
}
fmt.Println(sparse1Count,sparse3Count,intersection)
/ *
var z uint64 = 1

lineBytes := make([]byte,8)
lineptr := (*[8]byte)(unsafe.Pointer(&z))
fmt.Println(copy(lineBytes[:],(*lineptr)[0:8]))
fmt.Println(lineBytes)* /

wg.Wait()

}

func buildBitSet(num int, fileName string,nColumn int, size int) (*sparsebitset.BitSet){
var fieldDelimiter []byte = []byte("|")
var lineDelimiter []byte = []byte("\n")
datFile, err := os.Open(fileName)
if err != nil {
	log.Fatal(err)
}

bufReader := bufio.NewReaderSize(datFile,4096)
//bufReader.
//var lineString string;
var totalBytesRead uint64 = 0
var lineNumber uint32 = 0

c := make(chan bitsetservice.HRO)

psparse := sparsebitset.New(1)
//nsparse := sparsebitset.New(0)
wg.Add(1)
HLRoot := "c:/home/HL/"
hlPath := fmt.Sprintf("%v/%v/%v/",HLRoot,num,nColumn)
os.MkdirAll(hlPath,0)
go hashChunksWriter(hlPath,c)
//	matchSet := false;
//	z2:
for ; ; {
	buffer, err := bufReader.ReadSlice(lineDelimiter[0])
	//bytesRead, err := fmt.Fscan(datFile, &lineString)
	var hl = new(bitsetservice.HRO)
	if err != nil {
		if err == io.EOF {
			break
		}
		log.Fatal(err)
	}
	//fmt.Println(buffer)
	//6508
	lineNumber++
	chunks := bytes.Split(buffer,fieldDelimiter)
	chunkCount := len(chunks)
	if chunkCount != size {
	fmt.Println(lineNumber)
	}
	for  n,chunk := range(chunks) {
		if n == nColumn  {
			if n == chunkCount {
				chunk = chunk[:len(chunk)-1]
			}

			//hl.Hash = *(*uint64)(unsafe.Pointer(&(hs.Sum(chunk))[0]))
			hs.Reset()
			hs.Write(chunk)
			hl.Hash = hs.Sum64()

			/ *if lineNumber == 4478 {
				psparse.Set(hl.Hash)
			}* /

			psparse.Set(uint64(hl.Hash))
			//fmt.Println(psparse.Cardinality())


		/ *	if string(chunk)=="6508" {
				if match == 0 {
					match = hl.Hash
				} else {
					fmt.Println(match,hl.Hash)
					fmt.Println(psparse.Test(uint64(match)))
				}
				matchSet = true

				fmt.Println(lineNumber)
			}
			if match>0 && matchSet {
				if(!psparse.Test(uint64(match))) {
					fmt.Println(">",lineNumber)
					break z2;
				}
			}* /
			//hl.Hash = hs.Sum(chunk)[:]
			//fmt.Println(string(chunk),string(hl.Hash))

			hl.FileOffset = hl.FileOffset + uint64(totalBytesRead)

			//fmt.Println(string(chunk),string(hl.Hash))

			/ *hashValue := (*int64)(unsafe.Pointer(&hl.Hash))
			psparse.Set(*hashValue)
			if *signedHashValue > 0
				if *signedHashValue > 0 {
					psparse.Set(int(*signedHashValue))
				} else {
					usignedHashValue := (*uint64)(unsafe.Pointer(&hl.Hash))
					nsparse.Set(int(*usignedHashValue))
				}
			}
			*
			/

			//unsignedHashValue := (*uint64)(unsafe.Pointer(&hl.Hash[0]))
			//fmt.Println(string(chunk),string(hl.Hash),*unsignedHashValue )

			hl.RowNumber = lineNumber;
			//hl.Line = make([]byte, 8)
			//copy(hl.Line[:], ((*[8]byte)(unsafe.Pointer(&lineNumber)))[0:8])

			c <- *hl

			break
		}
		//hl.position = hl.position + uint64(len(chunk))
		//hl.position = hl.position + uint64(len(fieldDelimiter))
	}

	totalBytesRead = totalBytesRead + uint64(len(buffer));
	//fmt.Println(totalBytesRead)


	if ( false && lineNumber == 20) {
		break;
	}
	//splitValues := strings.Split(lineString, "|");





}
close(c)
fmt.Println("Done ",fileName)
//	fmt.Println(psparse.Test(uint64(match)))
return psparse
}


func hashChunksWriter(path string, in  chan bitsetservice. HRO) {
writers := make(map[string]io.Writer)
var hl bitsetservice.HRO

for hl = range in {
	//uval := (*uint64)(unsafe.Pointer(&hl.Hash[0]))

	key := ""//getHLKey(hl.Hash)
	//key := fmt.Sprintf("%X", hl.Hash & 0x0000FFFF);
//		fmt.Println(string(hl.Hash),*uval,key,hl.Line)
	var writer io.Writer;

	if foundWriter, found := writers[key]; !found {
		//hbytes:=(*(*[8]byte)(unsafe.Pointer(&hl.Hash)))[0:8]
		//fmt.Println(string(hbytes),hl.Hash ,key,hl.Line)
		//fmt.Println(path + key)
		file, error := os.Create(path + key)
		if error != nil {
			log.Fatal(error)
		}
		defer file.Close()
		writer = file;
		writers[key] = writer
	} else {
		writer = foundWriter;
	}
	writer.Write( (*(*[8]byte)(unsafe.Pointer(&hl.Hash)))[0:8] )
	writer.Write( (*(*[8]byte)(unsafe.Pointer(&hl.RowNumber)))[0:8] )
	writer.Write( (*(*[4]byte)(unsafe.Pointer(&hl.FileOffset)))[0:4] )
}
wg.Done()
}

func Unzip() {
file, err := os.OpenFile("C:/1/1.zip", os.O_RDONLY, 0)
if err != nil {
	log.Fatal(err)
}
defer file.Close()
fileInfo, err := file.Stat();
if err != nil {
	log.Fatal(err)
}
zr, err := zip.NewReader(file, fileInfo.Size());
//zr.RegisterDecompressor(zip.Deflate,func(in io.Reader)(io.ReadCloser,error){
//	return flate.Reader(in,flate.DefaultCompression)
//})
if err != nil {
	log.Fatal(err)
}

for _, zipped := range (zr.File) {
	fz, err := zipped.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer fz.Close()

	fuz, err := os.Create("c:/1/" + zipped.FileHeader.Name)
	if err != nil {
		log.Fatal(err)
	}
	defer fuz.Close()

	io.Copy(fuz, fz);
}
}

func Zip() {
start := time.Now()
file, err := os.Create("C:/1/1.zip");
if err != nil {
	log.Fatal(err)
}
defer file.Close()
z := zip.NewWriter(file)
zw, err := z.Create("data")
if err != nil {
	log.Fatal(err)
}
defer z.Close()
z.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
	return flate.NewWriter(out, flate.DefaultCompression);
})
for i := 0; i < 10001000; i++ {
	zw.Write([]byte(fmt.Sprintf("%v", i)))
}

fmt.Println(time.Since(start))
}

*/
