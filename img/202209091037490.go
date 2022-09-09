package main

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const maxUploadSize = 2 * 1024 * 1024 * 1024 // 2 gb
const uploadPath = "./tmp"

type EncInfo struct {
	EncKey string
	EncIV  string
}

type BeginEndSt struct {
	begin_time_index int
	end_time_index   int
}

type TransMediaInfo struct {
	TransID             uint           `db:"trans_id"`
	TransName           string         `db:"trans_name"`
	TransModel          string         `db:"trans_model"`
	TransType           string         `db:"trans_type"`
	TransOriginFileName string         `db:"trans_origin_file_name"`
	TransOutputFileName sql.NullString `db:"trans_output_file_name"`
	TransOriginFilePath sql.NullString `db:"trans_origin_file_path"`
	TransOutputFilePath sql.NullString `db:"trans_output_file_path"`
	TransCryptType      sql.NullString `db:"trans_crypt_type"`
	TransCryptKey       sql.NullString `db:"trans_crypt_key"`
	TransCryptIV        sql.NullString `db:"trans_crypt_iv"`
	TransStats          int            `db:"trans_stats"`
	TransStatsDesc      string         `db:"trans_stats_desc"`
	TransSubmitUser     sql.NullString `db:"trans_submit_user"`
	TransSubmitDate     sql.NullString `db:"trans_submit_date"`
	TransUpdateDate     sql.NullString `db:"trans_update_date"`
}

var Db *sqlx.DB

func init() {

	database, err := sqlx.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/video_convert_db")
	if err != nil {
		fmt.Println("open mysql failed,", err)
		return
	}

	Db = database
	//defer Db.Close() // 注意这行代码要写在上面err判断的下面
}

// func CORSMiddleware() gin.HandlerFunc {
// 	    return func(c *gin.Context) {
// 	        c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
// 	        c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
// 	        c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
// 	        c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")
// 	        if c.Request.Method == "OPTIONS" {
// 	            c.AbortWithStatus(204)
// 	            return
// 	        }
// 	        c.Next()
// 	    }
// 	}
// 	 func CORSMiddleware(handler http.Handler) http.Handler {
// 		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		  logger.Printf("path:%s process start...\n", r.URL.Path)
// 		  defer func() {
// 			logger.Printf("path:%s process end...\n", r.URL.Path)
// 		  }()
// 		  handler.ServeHTTP(w, r)
// 		})
// 	  }
func CORSMiddleware2(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
		} else {
			handler.ServeHTTP(w, r)
		}
	})
}

func CORSMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
		} else {
			handler(w, r)
		}
	}
}

func main() {
	http.HandleFunc("/", CORSMiddleware(indexHandler()))
	http.HandleFunc("/upload", CORSMiddleware(uploadFileHandler()))
	http.HandleFunc("/play", CORSMiddleware(playFileHandler()))
	//http.HandleFunc("/getEncyptInfo", CORSMiddleware(getEncyptInfoHandler()))
	http.HandleFunc("/getPlayList", CORSMiddleware(getPlayListHandler()))
	fs := http.FileServer(http.Dir(uploadPath))
	http.Handle("/files/", CORSMiddleware2(http.StripPrefix("/files", fs)))

	log.Print("Server started on localhost:8080, use /upload for uploading files and /files/{fileName} for downloading")
	log.Fatal(http.ListenAndServe(":8080", nil))

	defer Db.Close()
}

func indexHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			t, _ := template.ParseFiles("index.gtpl")
			t.Execute(w, nil)
			return
		}
		//w.Write([]byte("SUCCESS" + strings.Split(newPath, ".")[0]))
	})
}

func getPlayListHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" || r.Method == "POST" {
			//返回所有匹配的文件
			match, _ := filepath.Glob(uploadPath + "/*.png")
			fmt.Println(match)
			//for i := 0; i < len(match); i++ {
			//	match[i] = strings.Split(strings.Split(match[i], ".")[0], "\\")[1]
			//}
			for i := 0; i < len(match); i++ {
				match[i] = strings.Split(match[i], "\\")[1]
			}
			b, _ := json.Marshal(match)
			w.Write(b)
		}
		//w.Write([]byte())
	})
}

//func getEncyptInfoHandler() http.HandlerFunc {
//	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//		if r.Method == "GET" || r.Method == "POST" {
//			//query := r.URL.Query()
//			//resId := query["id"]
//			encPath := filepath.Join(uploadPath, "enc.key")
//			encFile, err := os.Open(encPath)
//			if err != nil {
//				renderError(w, "CANT_READ_FILE", http.StatusInternalServerError)
//				return
//			}
//
//			defer encFile.Close() // idempotent, okay to call twice
//			var b []byte = make([]byte, 2*1024)
//			var count int = 0
//			if count, err = encFile.Read(b); err != nil || encFile.Close() != nil {
//				renderError(w, "CANT_READ_FILE", http.StatusInternalServerError)
//				return
//			}
//
//			sEncKey := base64.StdEncoding.EncodeToString(b[0 : count-1])
//			strIV := "4401d17cc1e2a351a7e065ccbf83ab10"
//			bIV, _ := hex.DecodeString(strIV)
//			sEncIV := base64.StdEncoding.EncodeToString(bIV)
//			encInfo := EncInfo{sEncKey, sEncIV}
//			strJEnc, _ := json.Marshal(encInfo)
//			w.Write(strJEnc)
//		}
//		//w.Write([]byte())
//	})
//}

func playFileHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			t, _ := template.ParseFiles("play.gtpl")
			t.Execute(w, nil)
			return
		}
		//w.Write([]byte("SUCCESS" + strings.Split(newPath, ".")[0]))
	})
}

func uploadFileHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			t, _ := template.ParseFiles("upload.gtpl")
			t.Execute(w, nil)
			return
		}
		if err := r.ParseMultipartForm(maxUploadSize); err != nil {
			fmt.Printf("Could not parse multipart form: %v\n", err)
			renderError(w, "CANT_PARSE_FORM", http.StatusInternalServerError)
			return
		}

		// parse and validate file and post parameters
		file, fileHeader, err := r.FormFile("uploadFile")
		if err != nil {
			renderError(w, "INVALID_FILE", http.StatusBadRequest)
			return
		}

		model := r.FormValue("model")
		trans_name := r.FormValue("trans_name")
		user := r.FormValue("user")
		filename := fileHeader.Filename
		file_ext := strings.Split(filename, ".")[1]

		res, err := Db.Exec("insert into trans_media_info(trans_name, trans_model, trans_type, trans_origin_file_name, trans_stats, trans_stats_desc, trans_submit_user, trans_submit_date, trans_update_date)values(?, ?, ?, ?, ?, ?, ?, now(), now())", trans_name, model, "hls", filename, 0, "init", user)
		if err != nil {
			fmt.Println("exec failed, ", err)
			return
		}
		id, err := res.LastInsertId()
		if err != nil {
			fmt.Println("exec failed, ", err)
			return
		}

		fmt.Println("insert succ:", id)

		defer file.Close()
		// Get and print out file size
		fileSize := fileHeader.Size
		fmt.Printf("File size (bytes): %v\n", fileSize)
		// validate file size
		if fileSize > maxUploadSize {
			renderError(w, "FILE_TOO_BIG", http.StatusBadRequest)
			return
		}
		fileBytes, err := ioutil.ReadAll(file)
		if err != nil {
			renderError(w, "INVALID_FILE", http.StatusBadRequest)
			return
		}

		//// check file type, detectcontenttype only needs the first 512 bytes
		//detectedFileType := http.DetectContentType(fileBytes)
		//switch detectedFileType {
		//case "video/mp4":
		//	break
		//default:
		//	renderError(w, "INVALID_FILE_TYPE", http.StatusBadRequest)
		//	return
		//}
		fileName := randToken(12) + file_ext
		//fileEndings, err := mime.ExtensionsByType(detectedFileType)
		//if err != nil {
		//	renderError(w, "CANT_READ_FILE_TYPE", http.StatusInternalServerError)
		//	return
		//}
		//newPath := filepath.Join(uploadPath, fileName+fileEndings[0])
		//fmt.Printf("FileType: %s, File: %s\n", detectedFileType, newPath)
		newPath := filepath.Join(uploadPath, fileName)
		// write file
		newFile, err := os.Create(newPath)
		if err != nil {
			renderError(w, "CANT_WRITE_FILE", http.StatusInternalServerError)
			return
		}

		defer newFile.Close() // idempotent, okay to call twice
		if _, err := newFile.Write(fileBytes); err != nil || newFile.Close() != nil {
			renderError(w, "CANT_WRITE_FILE", http.StatusInternalServerError)
			return
		}

		res, err = Db.Exec("update trans_media_info set trans_origin_file_path=?, trans_stats=?, trans_stats_desc=?,trans_update_date=now() where trans_id=?", newPath, 1, "file commited.", id)
		if err != nil {
			fmt.Println("exec failed, ", err)
			return
		}
		row, err := res.RowsAffected()
		if err != nil {
			fmt.Println("rows failed, ", err)
		}
		fmt.Println("update succ:", row)

		go getVideoHandle(newPath, id)

		w.Write([]byte("转码中，id为:" + strings.Split(strings.Split(newPath, ".")[0], "\\")[1]))
	})
}

func renderError(w http.ResponseWriter, message string, statusCode int) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(message))
}

func randToken(len int) string {
	b := make([]byte, len)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func execTransByStartEndTime(input string, video_name string, current_split_time BeginEndSt, enc_keyinfo_file_path string) (string, bool) {
	//arg := []string{
	//	"ffmpeg", "-y",
	//	"-i", input,
	//	"-hls_time", "12",
	//	"-hls_list_size", "0",
	//	"-hls_key_info_file", "enc.keyinfo",
	//	"-hls_segment_filename", video_name + "%03d.ts",
	//	video_name + ".m3u8",
	//}
	arg := []string{
		"ffmpeg", "-y",
		"-ss", strconv.Itoa(current_split_time.begin_time_index),
	}
	if current_split_time.end_time_index > 0 {
		arg = append(arg,
			"-t", strconv.Itoa(current_split_time.end_time_index),
		)
	}
	arg = append(arg,
		"-i", input,
		//"-vf", "scale=iw*min(720/iw\\,404/ih):ih*min(720/iw\\,404/ih),pad=720:404:(720-iw)/2:(404-ih)/2",
		"-i", "logo_gray20p.png",
		"-filter_complex", "scale=iw*min(720/iw\\,404/ih):ih*min(720/iw\\,404/ih),pad=720:404:(720-iw)/2:(404-ih)/2,overlay=x=W-w-20:y=10",
		"-s", "720x404", "-aspect", "16:9", "-f", "hls", "-hls_list_size", "0", "-hls_key_info_file", enc_keyinfo_file_path, "-b:v", "512k",
		"-profile:v", "high", "-level", "3.1", "-vcodec", "libx264", "-coder", "1", "-flags", "+loop",
		"-cmp", "+chroma", "-partitions", "+parti8x8+parti4x4+partp8x8-partb8x8", "-me_method", "umh", "-subq", "2",
		"-me_range", "16", "-g", "240", "-keyint_min", "23", "-sc_threshold", "40", "-i_qfactor", "0.71", "-qcomp", "0.6", "-qmin", "0", "-qmax", "69", "-qdiff", "4", "-bf", "3", "-refs", "3",
		"-trellis", "2", "-wpredp", "2", "-rc-lookahead", "40", "-maxrate", "512k", "-minrate", "200k", "-bufsize", "200k", "-af", "loudnorm=I=-18", "-ab", "64k", "-ac", "2",
		"-ar", "44100", "-hls_segment_filename", video_name+"%03d.ts", video_name+".m3u8",
	)

	cmd := exec.Command(arg[0], arg[1:]...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		errstr := stderr.String()
		fmt.Println(errstr)
		return errstr, false
	} else {
		retstr := out.String()
		fmt.Println(retstr)
		return retstr, true
	}
}

func compactM3u8File(destM3u8FilePath string, appendM3u8FilePath string, isLastOne bool, adM3u8FilePath string, isInsertAd bool, adPrefixStr string) (string, bool) {
	dest_m3u8_file, err := os.OpenFile(destM3u8FilePath, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return "can`t open destM3u8FilePath", false
	}
	append_data, err := os.ReadFile(appendM3u8FilePath)
	if err != nil {
		return "can`t read appendM3u8FilePath", false
	}

	if isInsertAd {
		ad_m3u8_data, err := os.ReadFile(adM3u8FilePath)
		if err != nil {
			return "can`t read adM3u8FilePath", false
		}
		current_dest_file_len, _ := dest_m3u8_file.Seek(0, os.SEEK_END)
		if current_dest_file_len == 0 {
			reg := regexp.MustCompile(`(?s:(.*?))\n#EXTINF:`)
			append_header := reg.FindStringSubmatch(string(append_data))
			dest_m3u8_file.Write([]byte(append_header[1] + "\n"))
			ad_reg := regexp.MustCompile(`#EXTINF:(?s:(.*?))\n#EXT-X-ENDLIST`)
			ad_text := ad_reg.FindStringSubmatch(string(ad_m3u8_data))
			str_ad := ad_text[1]
			str_ad = strings.Replace(str_ad, ",\n", ",NOENC\n"+adPrefixStr, -1)
			dest_m3u8_file.Write([]byte("#EXTINF:" + str_ad + "\n#EXT-X-DISCONTINUITY\n"))
			append_text := ad_reg.FindStringSubmatch(string(append_data))
			dest_m3u8_file.Write([]byte("#EXTINF:" + append_text[1] + "\n"))
		} else {
			return "can`t insert ad", false
		}
	} else {
		current_dest_file_len, _ := dest_m3u8_file.Seek(0, os.SEEK_END)
		if current_dest_file_len == 0 {
			reg := regexp.MustCompile(`(?s:(.*?))\n#EXT-X-ENDLIST`)
			text := reg.FindStringSubmatch(string(append_data))
			dest_m3u8_file.Write([]byte(text[1] + "\n"))
		} else {
			dest_m3u8_file.WriteString("#EXT-X-DISCONTINUITY\n")
			reg := regexp.MustCompile(`#EXTINF:(?s:(.*?))\n#EXT-X-ENDLIST`)
			text := reg.FindStringSubmatch(string(append_data))
			dest_m3u8_file.Write([]byte("#EXTINF:" + text[1] + "\n"))
		}
	}

	if isLastOne {
		dest_m3u8_file.WriteString("#EXT-X-ENDLIST\n")
	}
	dest_m3u8_file.Close()
	return "", true
}

func encM3u8File(originM3u8FilePath string, encM3u8FilePath string) (string, bool) {
	origin_data, err := os.ReadFile(originM3u8FilePath)
	if err != nil {
		return "can`t read originM3u8FilePath", false
	}
	enc_m3u8_file, err := os.OpenFile(encM3u8FilePath, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return "can`t open destM3u8FilePath", false
	}
	current_index, _ := enc_m3u8_file.Seek(0, os.SEEK_END)
	preenc_origin_data := origin_data[current_index:]
	enc_data := make([]byte, len(preenc_origin_data))
	for i := 0; i < len(preenc_origin_data); i++ {
		enc_data[i] = preenc_origin_data[i] ^ 0x8
	}
	enc_m3u8_file.Write(enc_data)
	enc_m3u8_file.Close()
	return "", true
}

func getVideoHandle(input string, id int64) (string, bool) {
	if input == "" || id < 0 {
		fmt.Println("parameters is wrong")
		return "parameters is wrong", false
	}

	var trans_media_info []TransMediaInfo
	dberr := Db.Select(&trans_media_info, "select * from trans_media_info where trans_id=?", id)
	if dberr != nil {
		errstr := "exec failed, " + dberr.Error()
		fmt.Println(errstr)
		return errstr, false
	}

	fmt.Println("select succ:", trans_media_info)

	query_video_arg := []string{
		"ffprobe",
		"-i", input,
	}
	query_video_cmd := exec.Command(query_video_arg[0], query_video_arg[1:]...)
	video_block_size := 5 * 1024 * 1024
	var time_list []BeginEndSt
	var query_video_out bytes.Buffer
	var query_video_stderr bytes.Buffer
	query_video_cmd.Stdout = &query_video_out
	query_video_cmd.Stderr = &query_video_stderr
	query_video_err := query_video_cmd.Run()
	if query_video_err != nil {
		errstr := query_video_stderr.String()
		fmt.Println(errstr)
		return errstr, false
	} else {
		retstr := query_video_out.String()
		fmt.Println(retstr)
		errstr := query_video_stderr.String()
		fmt.Println(errstr)

		// ffprobe 获取文件信息类似这种 Duration: 00:08:16.48, start: 0.000000, bitrate: 178 kb/s
		reg := regexp.MustCompile(`Duration: (?s:(.*?)), start: (?s:(.*?)), bitrate: (?s:(.*?)) kb/s`)
		text := reg.FindStringSubmatch(errstr)
		fmt.Println(text)
		str_duration := text[1]
		//str_starttime := text[2]
		//str_bitrate := text[3]       // 后续可考虑使用bitrate 比特率进行判断分片转码个数

		var times_s []string = strings.Split(str_duration, ":")
		time_1, _ := strconv.Atoi(times_s[0])
		time_2, _ := strconv.Atoi(times_s[1])
		time_3, _ := strconv.ParseFloat(times_s[2], 64)
		//bitrate, _ := strconv.ParseFloat(str_bitrate, 64)
		var time_second float64 = float64(time_1*3600) + float64(time_2*60) + time_3
		//var count_size float64 = time_second * bitrate
		fi, err := os.Stat(input)
		if err != nil {
			return "convert file not found", false
		}
		var count_size float64 = float64(fi.Size())
		var split_count int = int(count_size) / video_block_size
		if split_count <= 0 {
			split_count = 1
		}
		var split_time int = int(time_second) / split_count

		fmt.Println(split_time)
		for i := 0; i < split_count; i++ {
			fmt.Println(split_count)
			var be_time_st BeginEndSt
			if i == split_count-1 {
				be_time_st.begin_time_index = i * split_time
				be_time_st.end_time_index = -1
			} else {
				be_time_st.begin_time_index = i * split_time
				be_time_st.end_time_index = split_time
			}
			time_list = append(time_list, be_time_st)
		}

	}

	video_name := strings.Split(input, ".")[0]

	// 生成加密key以及文件
	video_key_path := video_name + ".key"
	video_enc_key_path := video_name + "_enc.key"
	video_key_info_path := video_name + ".keyinfo"
	video_key_file, err := os.OpenFile(video_key_path, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return "can`t create key", false
	}
	video_enc_key_file, err := os.OpenFile(video_enc_key_path, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return "can`t create key", false
	}
	b_video_key := make([]byte, 16)
	rand.Read(b_video_key)
	enc_b_video_key := make([]byte, 16)
	for i := 0; i < 16; i++ {
		enc_b_video_key[i] = b_video_key[i] ^ 0x5
	}

	video_key_file.Write(b_video_key)
	video_enc_key_file.Write(enc_b_video_key)

	video_key_file.Close()
	video_enc_key_file.Close()

	video_key_info_file, err := os.OpenFile(video_key_info_path, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return "can`t create keyinfo", false
	}

	video_key_info_file.WriteString(strings.Split(video_enc_key_path, "\\")[1] + "\n")
	video_key_info_file.WriteString(video_key_path + "\n")
	video_key_info_file.WriteString(randToken(16) + "\n")
	video_key_info_file.Close()

	// 执行加密分片转码
	for i := 0; i < len(time_list); i++ {
		retstr, ret := execTransByStartEndTime(input, video_name+"part"+strconv.Itoa(i), time_list[i], video_key_info_path)
		if !ret {
			return retstr, ret
		}
		compactM3u8File(video_name+".m3u8", video_name+"part"+strconv.Itoa(i)+".m3u8", i == len(time_list)-1, "", false, "")
		if i == 0 {
			compactM3u8File(video_name+"_ad.m3u8", video_name+"part"+strconv.Itoa(i)+".m3u8", i == len(time_list)-1, "tmp//aboutVcom.m3u8", true, "../../../../../../../files/")
		} else {
			compactM3u8File(video_name+"_ad.m3u8", video_name+"part"+strconv.Itoa(i)+".m3u8", i == len(time_list)-1, "", false, "")
		}

		encM3u8File(video_name+".m3u8", video_name+".png")
		encM3u8File(video_name+"_ad.m3u8", video_name+"_ad.png")
	}

	return "", true

	//	arg := []string{
	//		"ffmpeg", "-y",
	//		"-i", input,
	//		"-hls_time", "12",
	//		"-hls_list_size", "0",
	//		"-hls_key_info_file", "enc.keyinfo",
	//		"-hls_segment_filename", video_name + "%03d.ts",
	//		video_name + ".m3u8",
	//	}
	//
	//	cmd := exec.Command(arg[0], arg[1:]...)
	//	var out bytes.Buffer
	//	var stderr bytes.Buffer
	//	cmd.Stdout = &out
	//	cmd.Stderr = &stderr
	//	err := cmd.Run()
	//	if err != nil {
	//		errstr := stderr.String()
	//		fmt.Println(errstr)
	//		return errstr, false
	//	} else {
	//		retstr := out.String()
	//		fmt.Println(retstr)
	//		return retstr, true
	//	}
}
