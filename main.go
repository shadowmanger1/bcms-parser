package main

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/secsy/goftp"
)

type Report struct {
	DateStamp    string
	TimeStamp    int64
	Number       int
	Name         string
	ServiceLevel int
	FileName     string
	SwitchName   string
	SplitRecords []SplitReportRecord
	TrunkRecords []TrunkReportRecord
	AgentRecords []AgentReportRecord
	VDNRecords   []VDNReportRecord
	Trunks       int
	ReportType   string
}

type SplitReportRecord struct {
	Time                  int64
	ACDCalls              int
	AvgSpeedAns           int
	AbandCalls            int
	AvgAbandTime          int
	AvgTalkTime           int
	TotalAfterCall        int
	FlowIn                int
	FlowOut               int
	TotalAUX              int
	AvgStaffed            float64
	InServiceLevelPercent int
}
type TrunkReportRecord struct {
	Time             int64
	IncomingCalls    int
	IncomingAband    int
	IncomingTime     int
	IncomingCCS      float64
	OutgoingCalls    int
	OutgoingComp     int
	OutgoingTime     int
	OutgoingCCS      float64
	AllBusyPercent   int
	TimeMaintPercent int
}
type AgentReportRecord struct {
	Time             int64
	ACDCalls         int
	AvgTalkTime      int
	TotalAfterCall   int
	TotalAvailTime   int
	TotalAUXOther    int
	ExtnCalls        int
	AvgExtnTime      int
	TotalTimeStaffed int
	TotalHoldTime    int
}
type VDNReportRecord struct {
	Time             int64
	CallsOffered     int
	ACDCalls         int
	AvgSpeedAns      int
	AbandCalls       int
	AvgAbandTime     int
	AvgTalkHold      int
	ConnCalls        int
	FlowOut          int
	BusyDisc         int
	InServLvlPercent int
}

func (report *Report) AddSplitRecord(record SplitReportRecord) []SplitReportRecord {
	report.SplitRecords = append(report.SplitRecords, record)
	return report.SplitRecords
}
func (report *Report) AddAgentRecord(record AgentReportRecord) []AgentReportRecord {
	report.AgentRecords = append(report.AgentRecords, record)
	return report.AgentRecords
}
func (report *Report) AddVDNRecord(record VDNReportRecord) []VDNReportRecord {
	report.VDNRecords = append(report.VDNRecords, record)
	return report.VDNRecords
}
func (report *Report) AddTrunkRecord(record TrunkReportRecord) []TrunkReportRecord {
	report.TrunkRecords = append(report.TrunkRecords, record)
	return report.TrunkRecords
}

func convertClock(st string) int {
	var m, s int
	n, err := fmt.Sscanf(st, "%d:%d", &m, &s)
	if err != nil || n != 2 {
		return 0
	}
	return m*60 + s
}

func convertDatetime(dateval string, timeval string) int64 {
	day := strings.Split(dateval, ",")[0][4:]
	month := dateval[:3]
	year := strings.TrimSpace(strings.Split(dateval, ",")[1])
	dateval = fmt.Sprintf("%s %02s, %s", month, day, year)
	timevals := strings.Split(timeval, " ")
	clock := strings.Split(timevals[0], ":")
	value := fmt.Sprintf("%s %02s:%s%s +0300", dateval, clock[0], clock[1], strings.ToUpper(timevals[1]))
	layout := "Jan 02, 2006 03:04PM -0700"
	t, _ := time.Parse(layout, value)
	t = t.AddDate(0, 0, -1) // Substructing one day because report for the day generates after midnight
	if t.Unix() < 0 {
		println(dateval)
		println(t.Unix())
		panic("h")
	}
	return t.Unix()
}

func convertTimeInterval(dateval string, interval string) int64 {
	timeval := "12:00 am"
	timestamp := convertDatetime(dateval, timeval)
	hours, _ := strconv.ParseInt(strings.Split(interval, ":")[0], 10, 64)
	return hours*3600 + timestamp
}

func parseFile(file os.FileInfo, client *goftp.Client, ftpServerPath string) {
	if strings.Contains(file.Name(), "bcms_sp") {
		buf := new(bytes.Buffer)
		fullFilePath := ftpServerPath + file.Name()
		err := client.Retrieve(fullFilePath, buf)
		if err != nil {
			panic(err)
		}
		lines := strings.Split(buf.String(), "\n")

		var r Report
		println(file.Name())
		r.DateStamp = strings.TrimSpace(lines[2][65:78])
		r.TimeStamp = convertDatetime(r.DateStamp, lines[2][52:60])
		r.Number, _ = strconv.Atoi(strings.TrimSpace(lines[3][13:44]))
		r.Name = strings.TrimSpace(lines[4][13:45])
		r.ServiceLevel, _ = strconv.Atoi(strings.TrimSpace(lines[4][74:78]))
		r.FileName = file.Name()
		r.SwitchName = strings.TrimSpace(lines[2][13:44])
		r.ReportType = "Split"

		var reportLines []string = append(lines[10:19], append(lines[32:41], lines[54:60]...)...)

		for _, line := range reportLines {
			var record SplitReportRecord
			record.Time = convertTimeInterval(r.DateStamp, strings.TrimSpace(line[0:11]))
			record.ACDCalls, _ = strconv.Atoi(strings.TrimSpace(line[12:17]))
			record.AvgSpeedAns = convertClock(strings.TrimSpace(line[18:23]))
			record.AbandCalls, _ = strconv.Atoi(strings.TrimSpace(line[25:29]))
			record.AvgAbandTime = convertClock(strings.TrimSpace(line[30:35]))
			record.AvgTalkTime = convertClock(strings.TrimSpace(line[36:41]))
			record.TotalAfterCall = convertClock(strings.TrimSpace(line[42:49]))
			record.FlowIn, _ = strconv.Atoi(strings.TrimSpace(line[50:54]))
			record.FlowOut, _ = strconv.Atoi(strings.TrimSpace(line[55:59]))
			record.TotalAUX = convertClock(strings.TrimSpace(line[60:67]))
			record.AvgStaffed, _ = strconv.ParseFloat(strings.TrimSpace(line[68:73]), 64)
			record.InServiceLevelPercent, _ = strconv.Atoi(strings.TrimSpace(line[75:78]))

			r.AddSplitRecord(record)
		}
		updateTables(r)
	} else if strings.Contains(file.Name(), "bcms_tru") {
		buf := new(bytes.Buffer)
		fullFilePath := ftpServerPath + file.Name()
		err := client.Retrieve(fullFilePath, buf)
		if err != nil {
			panic(err)
		}
		lines := strings.Split(buf.String(), "\n")

		var r Report
		println(file.Name())
		r.DateStamp = strings.TrimSpace(lines[2][65:78])
		r.TimeStamp = convertDatetime(r.DateStamp, lines[2][52:60])
		r.Number, _ = strconv.Atoi(strings.TrimSpace(lines[3][13:44]))
		r.Name = strings.TrimSpace(lines[4][13:45])
		r.Trunks, _ = strconv.Atoi(strings.TrimSpace(lines[4][74:78]))
		r.FileName = file.Name()
		r.SwitchName = strings.TrimSpace(lines[2][13:44])
		r.ReportType = "Trunk"

		var reportLines []string = append(lines[10:19], append(lines[32:41], lines[54:57]...)...)

		for _, line := range reportLines {
			var record TrunkReportRecord
			record.Time = convertTimeInterval(r.DateStamp, strings.TrimSpace(line[0:11]))
			record.IncomingCalls, _ = strconv.Atoi(strings.TrimSpace(line[12:17]))
			record.IncomingAband, _ = strconv.Atoi(strings.TrimSpace(line[18:23]))
			record.IncomingTime = convertClock(strings.TrimSpace(line[24:30]))
			record.IncomingCCS, _ = strconv.ParseFloat(strings.TrimSpace(line[31:39]), 64)
			record.OutgoingCalls, _ = strconv.Atoi(strings.TrimSpace(line[40:45]))
			record.OutgoingComp, _ = strconv.Atoi(strings.TrimSpace(line[46:51]))
			record.OutgoingTime = convertClock(strings.TrimSpace(line[52:58]))
			record.OutgoingCCS, _ = strconv.ParseFloat(strings.TrimSpace(line[59:67]), 64)
			record.AllBusyPercent, _ = strconv.Atoi(strings.TrimSpace(line[69:72]))
			record.TimeMaintPercent, _ = strconv.Atoi(strings.TrimSpace(line[75:78]))

			r.AddTrunkRecord(record)
		}
		updateTables(r)
	} else if strings.Contains(file.Name(), "bcms_ag_") {

		buf := new(bytes.Buffer)
		fullFilePath := ftpServerPath + file.Name()
		err := client.Retrieve(fullFilePath, buf)
		if err != nil {
			panic(err)
		}
		lines := strings.Split(buf.String(), "\n")

		var r Report
		println(file.Name())
		r.DateStamp = strings.TrimSpace(lines[2][65:78])
		r.TimeStamp = convertDatetime(r.DateStamp, lines[2][52:60])
		r.Number, _ = strconv.Atoi(strings.TrimSpace(lines[3][13:44]))
		r.Name = strings.TrimSpace(lines[4][13:45])
		r.FileName = file.Name()
		r.SwitchName = strings.TrimSpace(lines[2][13:44])
		r.ReportType = "Agent"

		var reportLines []string = append(lines[10:19], append(lines[32:41], lines[54:60]...)...)

		for _, line := range reportLines {
			var record AgentReportRecord
			record.Time = convertTimeInterval(r.DateStamp, strings.TrimSpace(line[0:11]))
			record.ACDCalls, _ = strconv.Atoi(strings.TrimSpace(line[12:17]))
			record.AvgTalkTime = convertClock(strings.TrimSpace(line[18:24]))
			record.TotalAfterCall = convertClock(strings.TrimSpace(line[25:32]))
			record.TotalAvailTime = convertClock(strings.TrimSpace(line[33:40]))
			record.TotalAUXOther = convertClock(strings.TrimSpace(line[41:48]))
			record.ExtnCalls, _ = strconv.Atoi(strings.TrimSpace(line[49:54]))
			record.AvgExtnTime = convertClock(strings.TrimSpace(line[55:61]))
			record.TotalTimeStaffed = convertClock(strings.TrimSpace(line[62:69]))
			record.TotalHoldTime = convertClock(strings.TrimSpace(line[70:77]))

			r.AddAgentRecord(record)
		}
		updateTables(r)
	} else if strings.Contains(file.Name(), "bcms_vdn_") {
		if strings.Contains(file.Name(), "day") {
			return
		}
		buf := new(bytes.Buffer)
		fullFilePath := ftpServerPath + file.Name()
		err := client.Retrieve(fullFilePath, buf)
		if err != nil {
			panic(err)
		}
		lines := strings.Split(buf.String(), "\n")

		var r Report
		println(file.Name())
		r.DateStamp = strings.TrimSpace(lines[2][65:78])
		r.TimeStamp = convertDatetime(r.DateStamp, lines[2][52:60])
		r.Number, _ = strconv.Atoi(strings.TrimSpace(lines[3][13:44]))
		r.Name = strings.TrimSpace(lines[4][13:45])
		r.FileName = file.Name()
		r.SwitchName = strings.TrimSpace(lines[2][13:44])
		r.ReportType = "VDN"
		r.ServiceLevel, _ = strconv.Atoi(strings.TrimSpace(lines[4][74:78]))

		var reportLines []string = append(lines[10:19], append(lines[32:41], lines[54:60]...)...)

		for _, line := range reportLines {
			var record VDNReportRecord
			record.Time = convertTimeInterval(r.DateStamp, strings.TrimSpace(line[0:11]))
			record.CallsOffered, _ = strconv.Atoi(strings.TrimSpace(line[13:19]))
			record.ACDCalls, _ = strconv.Atoi(strings.TrimSpace(line[20:25]))
			record.AvgSpeedAns = convertClock(strings.TrimSpace(line[26:31]))
			record.AbandCalls, _ = strconv.Atoi(strings.TrimSpace(line[32:37]))
			record.AvgAbandTime = convertClock(strings.TrimSpace(line[38:43]))
			record.AvgTalkHold = convertClock(strings.TrimSpace(line[44:49]))
			record.ConnCalls, _ = strconv.Atoi(strings.TrimSpace(line[50:56]))
			record.FlowOut, _ = strconv.Atoi(strings.TrimSpace(line[57:62]))
			record.BusyDisc, _ = strconv.Atoi(strings.TrimSpace(line[63:68]))
			record.InServLvlPercent, _ = strconv.Atoi(strings.TrimSpace(line[70:73]))

			r.AddVDNRecord(record)
		}
		updateTables(r)
	}
}

func updateTables(r Report) {
	var sqlRecords string
	switch r.ReportType {
	case "Split":
		sqlRecords = `INSERT INTO public.splitreportrecords (time, acdcalls, avgspeedans, abandcalls, avgabandtime, avgtalktime, totalaftercall, flowin, flowout, totalaux, avgstaffed, inservicelevelpercent, number, name, servicelevel, switchname, filename) VALUES`
		for _, record := range r.SplitRecords {
			sqlRecords += fmt.Sprintf(" (%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %d, %d, '%s', %d, '%s', '%s'),",
				record.Time,
				record.ACDCalls,
				record.AvgSpeedAns,
				record.AbandCalls,
				record.AvgAbandTime,
				record.AvgTalkTime,
				record.TotalAfterCall,
				record.FlowIn,
				record.FlowOut,
				record.TotalAUX,
				record.AvgStaffed,
				record.InServiceLevelPercent,
				r.Number,
				r.Name,
				r.ServiceLevel,
				r.SwitchName,
				r.FileName)
		}
	case "Agent":
		sqlRecords = `INSERT INTO public.agentreportrecords (time, acdcalls, avgtalktime, totalaftercall, totalavailtime, totalauxother, extncalls, avgextntime, totaltimestaffed, totalholdtime, number, name, servicelevel, switchname, filename) VALUES`
		for _, record := range r.AgentRecords {
			sqlRecords += fmt.Sprintf(" (%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, '%s', %d, '%s', '%s'),",
				record.Time,
				record.ACDCalls,
				record.AvgTalkTime,
				record.TotalAfterCall,
				record.TotalAvailTime,
				record.TotalAUXOther,
				record.ExtnCalls,
				record.AvgExtnTime,
				record.TotalTimeStaffed,
				record.TotalHoldTime,
				r.Number,
				r.Name,
				r.ServiceLevel,
				r.SwitchName,
				r.FileName)
		}
	case "Trunk":
		sqlRecords = `INSERT INTO public.trunkreportrecords (time, incomingcalls, incomingaband, incomingtime, incomingccs, outgoingcalls, outgoingcomp, outgoingtime, outgoingccs, allbusypercent, timemaintpercent, trunks, number, name, servicelevel, switchname, filename) VALUES`
		for _, record := range r.TrunkRecords {
			sqlRecords += fmt.Sprintf(" (%d, %d, %d, %d, %f, %d, %d, %d, %f, %d, %d, %d, %d, '%s', %d, '%s', '%s'),",
				record.Time,
				record.IncomingCalls,
				record.IncomingAband,
				record.IncomingTime,
				record.IncomingCCS,
				record.OutgoingCalls,
				record.OutgoingComp,
				record.OutgoingTime,
				record.OutgoingCCS,
				record.AllBusyPercent,
				record.TimeMaintPercent,
				r.Trunks,
				r.Number,
				r.Name,
				r.ServiceLevel,
				r.SwitchName,
				r.FileName)
		}
	case "VDN":
		sqlRecords = `INSERT INTO public.vdnreportrecords (time, callsoffered, acdcalls, avgspeedans, abandcalls, avgabandtime, avgtalkhold, conncalls, flowout, busydisc, inservlvlpercent, number, name, servicelevel, switchname, filename) VALUES`
		for _, record := range r.VDNRecords {
			sqlRecords += fmt.Sprintf(" (%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, '%s', %d, '%s', '%s'),",
				record.Time,
				record.CallsOffered,
				record.ACDCalls,
				record.AvgSpeedAns,
				record.AbandCalls,
				record.AvgAbandTime,
				record.AvgTalkHold,
				record.ConnCalls,
				record.FlowOut,
				record.BusyDisc,
				record.InServLvlPercent,
				r.Number,
				r.Name,
				r.ServiceLevel,
				r.SwitchName,
				r.FileName)
		}
	}
	sqlRecords = sqlRecords[:len(sqlRecords)-1] + " RETURNING 1;"
	sqlReports := fmt.Sprintf(`INSERT INTO public.reports (filename) VALUES ('%s') RETURNING 1;`, r.FileName)

	connectionString := os.Getenv("DATABASE_URL") // DATABASE_URL "postgres://username:password@localhost:5432/database_name"
	dbpool, err := pgxpool.Connect(context.Background(), connectionString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	returnVal := 0
	err = dbpool.QueryRow(context.Background(), sqlReports).Scan(&returnVal)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	err = dbpool.QueryRow(context.Background(), sqlRecords).Scan(&returnVal)
	if err != nil && err != pgx.ErrNoRows {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
}

func excludeParcedFiles(files []os.FileInfo) []os.FileInfo {
	return xor(files, getParsedFiles())
}

func getParsedFiles() (res []string) {
	connectionString := os.Getenv("DATABASE_URL") // DATABASE_URL "postgres://username:password@localhost:5432/database_name"
	dbpool, err := pgxpool.Connect(context.Background(), connectionString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	rows, err := dbpool.Query(context.Background(), `SELECT filename FROM public.reports`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var fileName string
	for rows.Next() {
		err := rows.Scan(&fileName)
		if err != nil {
			panic(err)
		}
		res = append(res, fileName)
	}
	return res
}

func xor(list1 []fs.FileInfo, list2 []string) []fs.FileInfo {
	set1 := make(map[string]bool)
	for _, s := range list1 {
		set1[s.Name()] = true
	}
	set2 := make(map[string]bool)
	for _, s := range list2 {
		set2[s] = true
	}

	var c []fs.FileInfo
	for _, s := range list1 {
		if !set2[s.Name()] {
			c = append(c, s)
		}
	}

	return c
}

func main() {
	ftpServerURL := os.Getenv("FTP_IP")   // FTP_IP "10.249.32.5"
	ftpServerPath := os.Getenv("FTP_DIR") // FTP_DIR "/BCMS 103/"
	username := os.Getenv("FTP_USERNAME") // FTP_USERNAME "pbx103"
	password := os.Getenv("FTP_PASSWORD") // FTP_PASSWORD "pbx10301"

	config := goftp.Config{
		User:               username,
		Password:           password,
		ConnectionsPerHost: 15,
		Timeout:            10 * time.Second,
		Logger:             os.Stderr,
	}

	client, err := goftp.DialConfig(config, ftpServerURL)
	if err != nil {
		panic(err)
	}

	files, err := client.ReadDir(ftpServerPath)
	files = excludeParcedFiles(files)

	if err != nil {
		panic(err)
	}

	maxGoroutines := 15
	guard := make(chan struct{}, maxGoroutines)

	var wg sync.WaitGroup
	for _, file := range files {
		guard <- struct{}{} // would block if guard channel is already filled
		wg.Add(1)
		go func(file fs.FileInfo) {
			defer wg.Done()
			parseFile(file, client, ftpServerPath)
			<-guard
		}(file)
	}
	wg.Wait()

	fmt.Println("done")
}
