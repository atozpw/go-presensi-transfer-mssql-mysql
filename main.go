package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
)

type Userinfo struct {
	Userid               int
	Badgenumber          string
	Name                 string
	Defaultdeptid        int
	Att                  int
	Inlate               int
	Outearly             int
	Overtime             int
	Sep                  int
	Holiday              int
	Lunchduration        int
	Privilege            int
	Inheritdeptsch       int
	Inheritdepthschclass int
	Autoschplan          int
	Minautoschinterval   int
	Registerot           int
	Inheritdeptrule      int
	Emprivilege          int
	Facegroup            int
	Accgroup             int
	Useaccgrouptz        int
	Verifycode           int
	Expires              int
	Validcount           int
	Timezone1            int
	Timezone2            int
	Timezone3            int
	Fselected            bool
}

type Checkinout struct {
	Userid      int
	Checktime   time.Time
	Checktype   sql.NullString
	Verifycode  int
	Sensorid    sql.NullString
	Memoinfo    sql.NullString
	Workcode    sql.NullString
	Sn          sql.NullString
	Userextfmt  int
	MaskFlag    int
	Temperature int
}

type CheckinoutPeriod struct {
	Userid     int
	Checktime  time.Time
	Checktype  sql.NullString
	Verifycode int
	Sensorid   sql.NullString
	Memoinfo   sql.NullString
	Workcode   sql.NullString
	Sn         sql.NullString
	Userextfmt int
}

func main() {

	c := cron.New()

	// Schedule tiap hari pukul 09:30 (format cron: "30 9 * * *")
	_, err := c.AddFunc("30 9 * * *", func() {
		Migrate()
	})
	if err != nil {
		log.Fatal("Gagal menjadwalkan job:", err)
	}

	// Jalankan cron scheduler
	c.Start()
	log.Println("Scheduler aktif. Menunggu jadwal eksekusi...")

	// Biar aplikasi tetap hidup
	select {}

}

func Migrate() {

	// --- Setup file log ---
	os.MkdirAll("logs", 0755)
	logFileName := fmt.Sprintf("logs/%s.log", time.Now().Format("20060102150405"))
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Gagal membuka file log: %v", err)
	}
	defer logFile.Close()

	// arahkan log output ke file dan ke console (optional)
	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile) // tampilkan waktu + baris kode

	// Load .env file
	errEnv := godotenv.Load()
	if errEnv != nil {
		log.Fatalf("Error loading .env file: %v", errEnv)
	}

	// --- Koneksi SQL Server ---
	srcDB, err := sql.Open("sqlserver", "sqlserver://"+os.Getenv("DB_MSSQL_USERNAME")+":"+os.Getenv("DB_MSSQL_PASSWORD")+"@"+os.Getenv("DB_MSSQL_HOST")+":"+os.Getenv("DB_MSSQL_PORT")+"?database="+os.Getenv("DB_MSSQL_DATABASE"))
	if err != nil {
		log.Fatal("SQL Server error:", err)
	}
	defer srcDB.Close()

	// --- Koneksi MySQL ---
	destDB, err := sql.Open("mysql", os.Getenv("DB_MYSQL_USERNAME")+":"+os.Getenv("DB_MYSQL_PASSWORD")+"@tcp("+os.Getenv("DB_MYSQL_HOST")+":"+os.Getenv("DB_MYSQL_PORT")+")/"+os.Getenv("DB_MYSQL_DATABASE")+"?parseTime=true")
	if err != nil {
		log.Fatal("MySQL error:", err)
	}
	defer destDB.Close()

	// --- Clear eksisting data ---
	_, err = destDB.Exec("TRUNCATE checkinout")
	if err != nil {
		log.Fatal(err)
	}

	_, err = destDB.Exec("TRUNCATE userinfo")
	if err != nil {
		log.Fatal(err)
	}

	// --- Parameter ---
	const batchSizeU = 10000
	const workerCountU = 5 // jumlah goroutine worker
	offsetU := 0

	// --- Channel untuk kirim batch data ke worker ---
	batchChanU := make(chan []Userinfo, workerCountU)
	var wgU sync.WaitGroup

	// --- Jalankan worker ---
	for i := 0; i < workerCountU; i++ {
		wgU.Add(1)
		go func(workerID int) {
			defer wgU.Done()
			for batch := range batchChanU {
				err := insertBatchU(destDB, batch)
				if err != nil {
					log.Printf("Worker %d: Gagal insert batch (%v)\n", workerID, err)
				} else {
					log.Printf("Worker %d: Selesai insert %d baris\n", workerID, len(batch))
				}
			}
		}(i + 1)
	}

	totalU := 0
	for {
		query := fmt.Sprintf(`
			SELECT userid, badgenumber, name, defaultdeptid, att, inlate, outearly, overtime, sep, holiday, lunchduration, privilege, inheritdeptsch, inheritdeptschclass, autoschplan, minautoschinterval, registerot, inheritdeptrule, emprivilege, facegroup, accgroup, useaccgrouptz, verifycode, expires, validcount, timezone1, timezone2, timezone3, fselected
			FROM userinfo
			ORDER BY userid
			OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, offsetU, batchSizeU)

		rows, err := srcDB.Query(query)
		if err != nil {
			log.Fatal("Query error:", err)
		}

		var batch []Userinfo
		for rows.Next() {
			var r Userinfo
			if err := rows.Scan(&r.Userid, &r.Badgenumber, &r.Name, &r.Defaultdeptid, &r.Att, &r.Inlate, &r.Outearly, &r.Overtime, &r.Sep, &r.Holiday, &r.Lunchduration, &r.Privilege, &r.Inheritdeptsch, &r.Inheritdepthschclass, &r.Autoschplan, &r.Minautoschinterval, &r.Registerot, &r.Inheritdeptrule, &r.Emprivilege, &r.Facegroup, &r.Accgroup, &r.Useaccgrouptz, &r.Verifycode, &r.Expires, &r.Validcount, &r.Timezone1, &r.Timezone2, &r.Timezone3, &r.Fselected); err != nil {
				log.Println("Scan error:", err)
				continue
			}
			batch = append(batch, r)
		}
		rows.Close()

		if len(batch) == 0 {
			break // tidak ada lagi data
		}

		batchChanU <- batch // kirim ke worker
		totalU += len(batch)
		offsetU += batchSizeU

		log.Printf("📦 Kirim batch %d (total %d baris)\n", offsetU/batchSizeU, totalU)
	}

	close(batchChanU)
	wgU.Wait()

	log.Printf("✅ Selesai! Total baris dipindahkan: %d\n", totalU)

	// --- Parameter ---
	const batchSizeCP = 10000
	const workerCountCP = 5 // jumlah goroutine worker
	offsetCP := 0

	// --- Channel untuk kirim batch data ke worker ---
	batchChanCP := make(chan []CheckinoutPeriod, workerCountCP)
	var wgCP sync.WaitGroup

	// --- Jalankan worker ---
	for i := 0; i < workerCountCP; i++ {
		wgCP.Add(1)
		go func(workerID int) {
			defer wgCP.Done()
			for batch := range batchChanCP {
				err := insertBatchCP(destDB, batch)
				if err != nil {
					log.Printf("Worker %d: Gagal insert batch (%v)\n", workerID, err)
				} else {
					log.Printf("Worker %d: Selesai insert %d baris\n", workerID, len(batch))
				}
			}
		}(i + 1)
	}

	totalCP := 0
	for {
		query := fmt.Sprintf(`
			SELECT userid, checktime, checktype, verifycode, sensorid, memoinfo, workcode, sn, userextfmt 
			FROM v_checkinout_interval_4_months
			ORDER BY checktime
			OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, offsetCP, batchSizeCP)

		rows, err := srcDB.Query(query)
		if err != nil {
			log.Fatal("Query error:", err)
		}

		var batch []CheckinoutPeriod
		for rows.Next() {
			var r CheckinoutPeriod
			if err := rows.Scan(&r.Userid, &r.Checktime, &r.Checktype, &r.Verifycode, &r.Sensorid, &r.Memoinfo, &r.Workcode, &r.Sn, &r.Userextfmt); err != nil {
				log.Println("Scan error:", err)
				continue
			}
			batch = append(batch, r)
		}
		rows.Close()

		if len(batch) == 0 {
			break // tidak ada lagi data
		}

		batchChanCP <- batch // kirim ke worker
		totalCP += len(batch)
		offsetCP += batchSizeCP

		log.Printf("📦 Kirim batch %d (total %d baris)\n", offsetCP/batchSizeCP, totalCP)
	}

	close(batchChanCP)
	wgCP.Wait()

	log.Printf("✅ Selesai! Total baris dipindahkan: %d\n", totalCP)

	// --- Parameter ---
	const batchSizeC = 10000
	const workerCountC = 5 // jumlah goroutine worker
	offsetC := 0

	// --- Channel untuk kirim batch data ke worker ---
	batchChanC := make(chan []Checkinout, workerCountC)
	var wgC sync.WaitGroup

	// --- Jalankan worker ---
	for i := 0; i < workerCountC; i++ {
		wgC.Add(1)
		go func(workerID int) {
			defer wgC.Done()
			for batch := range batchChanC {
				err := insertBatchC(destDB, batch)
				if err != nil {
					log.Printf("Worker %d: Gagal insert batch (%v)\n", workerID, err)
				} else {
					log.Printf("Worker %d: Selesai insert %d baris\n", workerID, len(batch))
				}
			}
		}(i + 1)
	}

	totalC := 0
	for {
		query := fmt.Sprintf(`
			SELECT userid, checktime, checktype, verifycode, sensorid, memoinfo, workcode, sn, userextfmt, mask_flag, temperature 
			FROM checkinout
			ORDER BY checktime
			OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`, offsetC, batchSizeC)

		rows, err := srcDB.Query(query)
		if err != nil {
			log.Fatal("Query error:", err)
		}

		var batch []Checkinout
		for rows.Next() {
			var r Checkinout
			if err := rows.Scan(&r.Userid, &r.Checktime, &r.Checktype, &r.Verifycode, &r.Sensorid, &r.Memoinfo, &r.Workcode, &r.Sn, &r.Userextfmt, &r.MaskFlag, &r.Temperature); err != nil {
				log.Println("Scan error:", err)
				continue
			}
			batch = append(batch, r)
		}
		rows.Close()

		if len(batch) == 0 {
			break // tidak ada lagi data
		}

		batchChanC <- batch // kirim ke worker
		totalC += len(batch)
		offsetC += batchSizeC

		log.Printf("📦 Kirim batch %d (total %d baris)\n", offsetC/batchSizeC, totalC)
	}

	close(batchChanC)
	wgC.Wait()

	log.Printf("✅ Selesai! Total baris dipindahkan: %d\n", totalC)

}

func insertBatchC(db *sql.DB, batch []Checkinout) error {

	if len(batch) == 0 {
		return nil
	}

	var values []string
	for _, r := range batch {
		checktime := r.Checktime.Format("2006-01-02 15:04:05")
		checktype := strings.ReplaceAll(r.Checktype.String, "'", "''")
		sensorid := strings.ReplaceAll(r.Sensorid.String, "'", "''")
		memoinfo := strings.ReplaceAll(r.Memoinfo.String, "'", "''")
		workcode := strings.ReplaceAll(r.Workcode.String, "'", "''")
		sn := strings.ReplaceAll(r.Sn.String, "'", "''")
		values = append(values, fmt.Sprintf("(%d, '%s', '%s', %d, '%s', '%s', '%s', '%s', %d, %d, %d)", r.Userid, checktime, checktype, r.Verifycode, sensorid, memoinfo, workcode, sn, r.Userextfmt, r.MaskFlag, r.Temperature))
	}

	query := fmt.Sprintf("INSERT INTO checkinout (userid, checktime, checktype, verifycode, sensorid, memoinfo, workcode, sn, userextfmt, mask_flag, temperature) VALUES %s", strings.Join(values, ","))
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(query)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()

}

func insertBatchCP(db *sql.DB, batch []CheckinoutPeriod) error {

	if len(batch) == 0 {
		return nil
	}

	var values []string
	for _, r := range batch {
		checktime := r.Checktime.Format("2006-01-02 15:04:05")
		checktype := strings.ReplaceAll(r.Checktype.String, "'", "''")
		sensorid := strings.ReplaceAll(r.Sensorid.String, "'", "''")
		memoinfo := strings.ReplaceAll(r.Memoinfo.String, "'", "''")
		workcode := strings.ReplaceAll(r.Workcode.String, "'", "''")
		sn := strings.ReplaceAll(r.Sn.String, "'", "''")
		values = append(values, fmt.Sprintf("(%d, '%s', '%s', %d, '%s', '%s', '%s', '%s', %d)", r.Userid, checktime, checktype, r.Verifycode, sensorid, memoinfo, workcode, sn, r.Userextfmt))
	}

	query := fmt.Sprintf("INSERT INTO checkinout_interval_4_months (userid, checktime, checktype, verifycode, sensorid, memoinfo, workcode, sn, userextfmt) VALUES %s", strings.Join(values, ","))
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(query)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()

}

func insertBatchU(db *sql.DB, batch []Userinfo) error {

	if len(batch) == 0 {
		return nil
	}

	var values []string
	for _, r := range batch {
		badgenumber := strings.ReplaceAll(r.Badgenumber, "'", "''")
		name := strings.ReplaceAll(r.Name, "'", "''")

		fselected := 0
		if r.Fselected {
			fselected = 1
		}

		values = append(values, fmt.Sprintf("(%d, '%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)", r.Userid, badgenumber, name, r.Defaultdeptid, r.Att, r.Inlate, r.Outearly, r.Overtime, r.Sep, r.Holiday, r.Lunchduration, r.Privilege, r.Inheritdeptsch, r.Inheritdepthschclass, r.Autoschplan, r.Minautoschinterval, r.Registerot, r.Inheritdeptrule, r.Emprivilege, r.Facegroup, r.Accgroup, r.Useaccgrouptz, r.Verifycode, r.Expires, r.Validcount, r.Timezone1, r.Timezone2, r.Timezone3, fselected))
	}

	query := fmt.Sprintf("INSERT INTO userinfo (userid, badgenumber, name, defaultdeptid, att, inlate, outearly, overtime, sep, holiday, lunchduration, privilege, inheritdeptsch, inheritdeptschclass, autoschplan, minautoschinterval, registerot, inheritdeptrule, emprivilege, facegroup, accgroup, useaccgrouptz, verifycode, expires, validcount, timezone1, timezone2, timezone3, fselected) VALUES %s", strings.Join(values, ","))
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(query)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()

}
