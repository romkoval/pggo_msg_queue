package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	_ "github.com/lib/pq"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

var (
	msg_id = 0
)

func get_msg_types() []string {
	return []string{"type1", "type2", "type3"}
}

func main() {
	var cfg Config
	cfg.Init()
	setupLogger(cfg.logfile)

	db, err := connect_db(cfg)
	defer db.Close()
	if err != nil {
		log.Fatalln(err)
	}

	create_schema(db, cfg.schema)

	if cfg.fill_db > 0 && cfg.rate == 0 {
		fill_queue(err, db, cfg)
	}

	res_chan := make(chan *handle_result, 10)
	quit := make(chan int, 1)
	var wg sync.WaitGroup

	pb := mpb.New(mpb.WithWaitGroup(&wg))
	wg.Add(cfg.workers)

	total_pb := make_pbar(pb, cfg.fill_db, "total")

	run_handlers(cfg, pb, db, res_chan, quit, wg)

	if cfg.rate > 0 {
		wg.Add(1)
		go fill_queue_with_rate(cfg.rate, cfg.fill_db, db, &wg, quit)
	}

	print_results(res_chan, total_pb, db, cfg, quit)

	wg.Wait()
}

func get_file_txt(fname string) string {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatalln(err)
	}
	return string(data)
}

func create_schema(db *sql.DB, schema string) {
	_, err := db.Exec(get_file_txt(schema))
	if err != nil {
		log.Fatalln("create schema failed", err)
	}
	log.Println("table msg_queue created")
}

func fill_db_with_rand(db *sql.DB, data_size int64) (err error) {
	tx, err := db.Begin()
	defer tx.Rollback()

	rand.Seed(time.Now().UnixNano())
	msg_types := get_msg_types()

	stmt, err := tx.Prepare(
		`insert into msg_queue (msg_id, order_id, queue_type, status, time_added)
		values ($1, $2, $3, 'N', $4)`)
	defer stmt.Close()
	for i := int64(0); i < data_size; i++ {
		msg_id += 1
		_, err := stmt.Exec(msg_id, rand.Intn(2)+1, msg_types[rand.Intn(len(msg_types))], time.Now())
		if err != nil {
			log.Fatalln(err)
		}
	}
	tx.Commit()
	return nil
}

type handle_result struct {
	msg_id   int64
	order_id int64
	qtype    string
	dt       string
	worker   int
}

type worker_params struct {
	queue_type string
	worker_num int
	delmsg     bool
	pb         *mpb.Progress
	total_msg  int64
}

type msg_queue struct {
	msg_id   int64
	order_id int64
	qtype    string
}

func next4handle(tx *sql.Tx, next_sql string, params *worker_params) (*msg_queue, error) {
	row := tx.QueryRow(next_sql, params.queue_type)

	var msg_id int64
	var order_id int64
	var qtype string
	err := row.Scan(&msg_id, &qtype, &order_id)
	if err != nil {
		return nil, err
	}
	return &msg_queue{
		msg_id:   msg_id,
		order_id: order_id,
		qtype:    qtype,
	}, nil
}

func make_pbar(p *mpb.Progress, total int64, name string) *mpb.Bar {
	bar := p.AddBar(total,
		mpb.PrependDecorators(
			decor.Name(name),
		),
		mpb.AppendDecorators(
			decor.CurrentNoUnit("%d"),
		),
	)
	return bar
}

func handle_queue(db *sql.DB, params *worker_params, res chan *handle_result, quit chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	pb := make_pbar(params.pb, params.total_msg, fmt.Sprintf("worker %d [%s]", params.worker_num, params.queue_type))
	done := false
	next_sql := get_file_txt("next_msg.sql")

	for !done {
		tx, err := db.Begin()
		if err != nil {
			log.Fatalln(err)
		}

		mq, err := next4handle(tx, next_sql, params)
		if err != nil {
			tx.Rollback()
		}
		if err == sql.ErrNoRows {
			log.Println("Done handling", params.queue_type, "queue")
			time.Sleep(1000 * time.Millisecond)
		} else if err != nil {
			log.Fatalln("failed to get message to handle", err)
		} else {
			time.Sleep(time.Duration(rand.Intn(10)+1) * time.Millisecond)

			if params.delmsg {
				_, err = tx.Query("delete from msg_queue where msg_id = $1", mq.msg_id)
			} else {
				_, err = tx.Query("update msg_queue set time_processed = now() where msg_id = $1", mq.msg_id)
			}
			if err != nil {
				log.Fatalln(err)
			}

			tx.Commit()
			pb.Increment()
			res <- &handle_result{
				msg_id:   mq.msg_id,
				order_id: mq.order_id,
				dt:       time.Now().Format(time.StampMicro),
				qtype:    mq.qtype,
				worker:   params.worker_num,
			}
		}

		select {
		case <-quit:
			log.Println("done worker ", params.worker_num)
			done = true
		default:
		}
	}
}

func setupLogger(filename string) {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime | log.Lmicroseconds)
	if filename != "-" {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatalln(err)
		}
		log.SetOutput(f)
	}
}

func fill_queue_with_rate(rate float64, total int64, db *sql.DB, wg *sync.WaitGroup, quit chan int) {
	defer wg.Done()
	buket := ratelimit.NewBucketWithRate(rate, 1)
	done := false
	for !done {
		if td := buket.Take(1); td > 0 {
			log.Println("sleep for ", td)
			time.Sleep(td)
		}
		err := fill_db_with_rand(db, 1)
		if err != nil {
			log.Fatalln("unable to fill db", err)
		} else {
			log.Println("fill db with rand values OK")
		}

		select {
		case <-quit:
			log.Println("done msg queue inserter")
			done = true
		default:
		}
	}
}

type Config struct {
	dsn     string
	schema  string
	fill_db int64
	logfile string
	workers int
	rate    float64
	delmsg  bool
}

func (cfg *Config) Init() {
	dsn := flag.String("dsn", "postgres://test:test@localhost/test", "pg connect string")
	schema := flag.String("schema", "schema.sql", "pg schema to create table & indexes")
	fill_db := flag.Int64("fill-db", 1000, "fill db with rand data")
	logfile := flag.String("logfile", "msg_queue.log", "logfile name, use '-' for stdout")
	workers := flag.Int("workers", 5, "message queue workers num")
	rate := flag.Float64("rate", 0, "continue inserting values with rate per second")
	delmsg := flag.Bool("delmsg", false, "delete message from queue when handled")
	flag.Parse()

	*cfg = Config{
		dsn:     *dsn,
		schema:  *schema,
		fill_db: *fill_db,
		logfile: *logfile,
		workers: *workers,
		rate:    *rate,
		delmsg:  *delmsg,
	}
}

func connect_db(cfg Config) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.dsn)
	if err != nil {
		fmt.Println("Unable to open db: ", err)
	}
	if err = db.Ping(); err != nil {
		fmt.Println("Unable to connect db: ", err)
	}
	return db, err
}

func fill_queue(err error, db *sql.DB, cfg Config) {
	err = fill_db_with_rand(db, cfg.fill_db)
	if err != nil {
		log.Fatalln("unable to fill db", err)
	} else {
		log.Println("fill db with rand values OK")
	}
}

func print_results(res_chan chan *handle_result, total_pb *mpb.Bar, db *sql.DB, cfg Config, quit chan int) {
	cnt := int64(0)
	for res := range res_chan {
		total_pb.Increment()
		cnt += 1
		log.Printf("%#v\n", res)
		if cnt%10 == 0 {
			log.Printf("%#v\n", db.Stats())
		}
		if cnt == cfg.fill_db {
			close(quit)
			break
		}
	}
}

func run_handlers(cfg Config, pb *mpb.Progress, db *sql.DB, res_chan chan *handle_result, quit chan int, wg sync.WaitGroup) {
	mtypes := get_msg_types()
	for i := 1; i < cfg.workers+1; i++ {
		params := &worker_params{
			queue_type: mtypes[i%len(mtypes)],
			total_msg:  cfg.fill_db,
			worker_num: i,
			pb:         pb,
			delmsg:     cfg.delmsg,
		}
		go handle_queue(db, params, res_chan, quit, &wg)
	}
}
