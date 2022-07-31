package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"sync"
	"time"

	"encoding/csv"
	"encoding/json"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"archive/zip"
)

type connection struct {
	status          string
	state           connectionState
	pgConnectionURL string
	pg              *pgx.Conn
	tmpFn           *os.File
	tmpFilename     string
	fetchStart      time.Time
}

type model struct {
	conns          []*connection
	query          string
	outputFilename string
	fetchesAllDone bool
	archiveAllDone bool
}

var textStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Render

type connectionState rune

const (
	stateInit       connectionState = '🎬'
	stateConnecting                 = '🔌'
	stateConnected                  = '🔌'
	stateStartQuery                 = '🔍'
	stateError                      = '💣'
	stateFetching                   = '📝'
	stateComplete                   = '🏁'
)

func main() {
	var query, out string

	flag.StringVar(&query, "query", "", "SQL query to perform")
	flag.StringVar(&out, "output", "", "Output zip filename")
	flag.Parse()

	if query == "" || out == "" {
		fmt.Println("You must supply both -query and -output arguments")
		os.Exit(1)
	}

	urls := flag.Args()

	if len(urls) == 0 {
		fmt.Println("You must supply at least one postgresql connection URL to connect to")
		os.Exit(1)
	}

	if len(os.Getenv("DEBUG")) > 0 {
		f, err := tea.LogToFile("debug.log", "debug")
		if err != nil {
			fmt.Println("fatal:", err)
			os.Exit(1)
		}
		defer f.Close()
	} else {
		w, _ := os.Open(os.DevNull)
		log.SetOutput(w)
	}

	m := model{}
	m.query = query
	m.outputFilename = out

	for _, h := range urls {

		conn := connection{
			status:          "init",
			pgConnectionURL: h,
			pg:              nil,
		}
		m.conns = append(m.conns, &conn)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(m.conns))
	for _, conn := range m.conns {
		go func(conn *connection) {
			connectAndQuery(conn, m.query)
			log.Printf("done a connection")
			wg.Done()
		}(conn)
	}

	p := tea.NewProgram(m)
	finalExit := make(chan bool)
	go func() {
		if err := p.Start(); err != nil {
			fmt.Println("could not run program:", err)
			os.Exit(1)
		}
		log.Printf("tea program quit - sending exit signal")
		finalExit <- true
		log.Print("sent exit")
	}()

	log.Printf("start waiting for queries to finish")
	wg.Wait()
	p.Send(updateModel(true, false))
	log.Printf("queries are finished")

	// start the archiving
	go func() {
		log.Printf("start archiving")
		archive, err := os.Create(m.outputFilename)
		if err != nil {
			fmt.Printf("failed to create zip output: %s\n", err)
			os.Exit(1)
		}
		defer archive.Close()
		zipper := zip.NewWriter(archive)
		for _, c := range m.conns {
			if c.state != stateComplete {
				continue
			}
			w, _ := zipper.Create(pgConnectionURLtoFilename(c.pgConnectionURL))
			d, _ := os.Open(c.tmpFilename)
			bytes, err := io.Copy(w, d)
			if err != nil {
				panic(err)
			}
			log.Printf("copied %d", bytes)
			os.Remove(c.tmpFilename)
		}
		zipper.Close()

		//time.Sleep(time.Second * 5)
		p.Send(updateModel(true, true))
		log.Printf("done archiving, sending quit to tea")
		p.Send(tea.Quit())
	}()

	log.Printf("waiting for final exit indicator")
	<-finalExit
	log.Printf("main finishing now")
}

func connectToHost(url string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return conn, fmt.Errorf("could not connect to %s: %s", url, err)

	}
	return conn, nil
}

func connectAndQuery(c *connection, query string) {
	c.status = "connecting"
	c.state = stateConnecting
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	conn, err := pgx.Connect(ctx, c.pgConnectionURL)
	if err != nil {
		c.status = fmt.Sprintf("failed: %s", err)
		c.state = stateError
		cancel()
		return
	}
	c.pg = conn
	c.status = "connected"
	c.state = stateConnected
	cancel()

	runQuery(c, query)
}

func runQuery(c *connection, query string) {
	rowCount := 0
	c.status = "executing query"
	c.state = stateStartQuery
	c.fetchStart = time.Now()

	rows, err := c.pg.Query(context.Background(), query)
	if err != nil {
		c.status = fmt.Sprintf("failed to run query: %s", err)
		c.state = stateError

		return
	}

	w := &csv.Writer{}
	headerWritten := false
	for rows.Next() {
		rowCount++
		if !headerWritten {
			// open the file
			f, err := os.CreateTemp("", "sqlcsv")
			if err != nil {
				c.state = stateError
				c.status = fmt.Sprintf("could not open tmp file: %s", err)
				return
			}
			defer f.Close()
			c.tmpFn = f
			c.tmpFilename = f.Name()
			w = csv.NewWriter(f)
			log.Printf("writing to %s", f.Name())

			descs := rows.FieldDescriptions()

			headerData := make([]string, len(descs))
			for i, v := range descs {
				headerData[i] = string(v.Name)
			}
			w.Write(headerData)
			headerWritten = true
		}
		vals, err := rows.Values()
		if err != nil {
			c.status = fmt.Sprintf("failed to get row values: %s", err)
			c.state = stateError
			return
		}

		rowData := make([]string, len(vals))
		for i, v := range vals {
			switch thisVal := v.(type) {
			// things that can be strings
			case string, net.HardwareAddr:
				rowData[i] = fmt.Sprintf("%s", thisVal)
			case int32, int64, uint32, uint64, int8, uint8, big.Int:
				rowData[i] = fmt.Sprintf("%d", thisVal)
			case map[string]interface{}:
				b, err := json.Marshal(thisVal)
				if err != nil {
					rowData[i] = "bad data"
				} else {
					rowData[i] = string(b)
				}
			case pgtype.Numeric:
				out := &big.Rat{}
				thisVal.AssignTo(out)
				// how to get the numeric fp digits?
				rowData[i] = out.FloatString(5)
			case float32, float64:
				rowData[i] = fmt.Sprintf("%f", thisVal)
			case time.Time, time.Duration, *net.IPNet:
				rowData[i] = fmt.Sprintf("%s", thisVal)
			case bool:
				rowData[i] = fmt.Sprintf("%t", thisVal)

			case nil:
				rowData[i] = "[null]"
			default:
				rowData[i] = fmt.Sprintf("(%T): %v", thisVal, thisVal)
			}
		}

		perSec := ""
		dur := time.Now().Sub(c.fetchStart).Seconds()
		if dur > 0 {
			perSec = fmt.Sprintf("(%.2f/s)", float64(rowCount)/dur)
		}

		c.status = fmt.Sprintf("fetch %d rows %s", rowCount, perSec)
		c.state = stateFetching
		w.Write(rowData)
	}
	w.Flush()
	c.state = stateComplete
	c.status = fmt.Sprintf("fetched %d rows successfully in %.1f seconds", rowCount, time.Now().Sub(c.fetchStart).Seconds())
}

type RefreshDisplay time.Time

func doTick() tea.Cmd {
	return tea.Tick(time.Second/6, func(t time.Time) tea.Msg {
		return RefreshDisplay(t)
	})
}

type modelUpdate struct {
	fetchesFinishes bool
	archiveFinished bool
}

func updateModel(fetches, archive bool) tea.Msg {
	log.Printf("executing updateModel")
	return modelUpdate{
		fetchesFinishes: fetches,
		archiveFinished: archive,
	}
}

func (m model) Init() tea.Cmd {
	return doTick()
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case RefreshDisplay:
		return m, doTick()
	case modelUpdate:
		m.archiveAllDone = msg.archiveFinished
		m.fetchesAllDone = msg.fetchesFinishes
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			log.Printf("quit request received")
			return m, tea.Quit
		default:
			return m, nil
		}
	}
	return m, nil
}

func (m model) View() (s string) {

	for i := range m.conns {
		s += fmt.Sprintf("%c - %-30s %s\n",
			m.conns[i].state,
			m.conns[i].pgConnectionURL,
			textStyle(m.conns[i].status),
		)
	}

	if m.fetchesAllDone && !m.archiveAllDone {
		s += fmt.Sprintf("\nWriting archive to %s ...\n", m.outputFilename)
	}
	if m.archiveAllDone {
		s += fmt.Sprintf("\nFinished writing to %s\n", m.outputFilename)
	}

	return
}

func pgConnectionURLtoFilename(url string) string {
	connConfig, err := pgx.ParseConfig(url)
	if err != nil {
		panic(err)
	}
	host := connConfig.Host
	database := connConfig.Database
	return fmt.Sprintf("%s_%s.csv", host, database)
}
