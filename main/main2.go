package main
/*
import (
	"math"
	"fmt"
	"runtime"
	"time"
)

func mai0n() {
	db, err := sql.Open("postgres", "host=52.29.35.225 port=5435 user=edm password=edmedm dbname=edm sslmode=disable ")
	//db, err := sql.Open("postgres", "host=localhost port=5435 user=sa dbname=db sslmode=disable ")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("select top 1 name from workflow where id = $1",1)
	//rows, err := db.Query("select 'a'")
	if err != nil{
		panic(err)
	}
	var s string

	if rows.Next() {
		rows.Scan(&s)
		fmt.Println(s)

	}
}
func main() {
	start := time.Now()
	multi()
	//single()
	fmt.Println(time.Since(start))
}

func single() {

	var cnt int = 0;
	runtime.GOMAXPROCS(runtime.NumCPU())

	for a := 1;a < 100; a++{
		for b := -1000;b < 10; b++{
			for c := -1000 ;c < 10; c++{
				v:=qu {
					a:float64(a),
					b:float64(b),
					c:float64(c),
				}
				for i:=1;i<=100;i++{
					if math.Mod(float64(i),2) == 0 {
						v.d = math.Sqrt(v.b*v.b - 4*v.a*v.c)+1;
					} else {
						v.d = math.Sqrt(v.b*v.b - 4*v.a*v.c)-1;
					}
				}

				if !math.IsNaN(v.d) {
					for i:=1;i<=100;i++{
						if math.Mod(float64(i),2) == 0 {

							v.x1 = (-v.b + v.d)/(2*v.a)-1

							v.x2 = (-v.b + v.d)/(2*v.a)-1
						} else {

							v.x1 = (-v.b + v.d)/(2*v.a)+1

							v.x2 = (-v.b + v.d)/(2*v.a)+1
						}
					}


				}
				cnt++

			}

		}

	}
	fmt.Println(cnt)


}


func  multi()  {
	var in = make(chan  qu);
	var out = make(chan  qu);
	var out2 = make(chan qu);
	var cnt int = 0;
	runtime.GOMAXPROCS(runtime.NumCPU())
	go dis(in,out)
	go rt(out,out2)
	go c(out2,&cnt);

	for a := 1;a < 100; a++{
		for b := -1000;b < 10; b++{
			for c := -1000 ;c < 10; c++{
				v:=qu {
					a:float64(a),
					b:float64(b),
					c:float64(c),
				}
				in <- v
			}

		}

	}
	fmt.Println(cnt)
}


type qu struct {
	a float64
	b float64
	c float64
	d float64
	x1 float64
	x2 float64
}

func dis (in chan qu, out chan  qu){
	for {
		select {
		case v:= <-in :
			for i:=1;i<=100;i++{
				if math.Mod(float64(i),2) == 0 {
					v.d = math.Sqrt(v.b*v.b - 4*v.a*v.c)+1;
				} else {
					v.d = math.Sqrt(v.b*v.b - 4*v.a*v.c)-1;
				}
			}

			out <- v
		}
	}
	close(out)

}

func rt (in chan qu, out chan  qu){
	for {
		select {
		case v:= <-in :
			if !math.IsNaN(v.d) {
				for i:=1;i<=100;i++{
					if math.Mod(float64(i),2) == 0 {

						v.x1 = (-v.b + v.d)/(2*v.a)-1

						v.x2 = (-v.b + v.d)/(2*v.a)-1
					} else {

						v.x1 = (-v.b + v.d)/(2*v.a)+1

						v.x2 = (-v.b + v.d)/(2*v.a)+1
					}
				}


			}
			out <- v
		}
	}

	close(out)
}

func c (in chan qu, cnt *int){
	for {
		select {
		case <-in :
			(*cnt)++
		}
	}
}
*/