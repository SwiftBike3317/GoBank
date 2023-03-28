package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	_ "github.com/lib/pq"
)

const (
	defaultHost     = "localhost"
	defaultPort     = "5432"
	defaultUser     = "user"
	defaultPassword = "pass"
	defaultDBName   = "db"
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

type DepositTransaction struct {
	Id     int
	Amount float64
	W      http.ResponseWriter
	R      *http.Request
}
type WithdrawTransaction struct {
	Id     int
	Amount float64
	W      http.ResponseWriter
	R      *http.Request
}

type Amount struct {
	Amount float64 `json:"amount"`
}

var db *sql.DB
var queueDeposit = make(chan DepositTransaction, 100)
var queueWithdraw = make(chan WithdrawTransaction, 100)

func main() {
	fmt.Println("Starting server")
	host := getEnv("DB_HOST", defaultHost)
	port := getEnv("DB_PORT", defaultPort)
	user := getEnv("DB_USER", defaultUser)
	password := getEnv("DB_PASSWORD", defaultPassword)
	dbname := getEnv("DB_NAME", defaultDBName)
	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	var err error
	db, err = sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := createTable(); err != nil {
		panic(err)
	}
	go depositProcess()
	go withdrawProcess()

	router := mux.NewRouter()

	router.HandleFunc("/clients", GetClients).Methods("GET")
	router.HandleFunc("/client", CreateClient).Methods("POST")
	router.HandleFunc("/client/deposit", handleDepositRequest).Methods("PUT")
	router.HandleFunc("/client/withdraw", handleWithdrawRequest).Methods("PUT")

	log.Fatal(http.ListenAndServe(":8000", router))

}
func handleDepositRequest(w http.ResponseWriter, r *http.Request) {
	// Добавляем запрос в очередь
	var a Amount
	_ = json.NewDecoder(r.Body).Decode(&a)
	if a.Amount <= 0 {
		http.Error(w, "insufficient funds", http.StatusBadRequest)
		return
	}

	clientID := r.FormValue("client_id")
	ID, err := strconv.Atoi(clientID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tx := DepositTransaction{
		Id:     ID,
		Amount: a.Amount,
		W:      w,
		R:      r,
	}
	go func() {
		queueDeposit <- tx
	}()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Deposit accepted successfully"))
}

// clientID int, amount float64
func depositProcess() {
	for _ = range queueDeposit {
		client := <-queueDeposit

		tx, err := db.Begin()
		if err != nil {
			tx.Rollback()
			http.Error(client.W, err.Error(), http.StatusInternalServerError)
			return
		}

		// Получаем баланс
		var currentBalance sql.NullFloat64
		err = tx.QueryRow("SELECT balance FROM clients WHERE id = $1", client.Id).Scan(&currentBalance)
		if err != nil {
			tx.Rollback()
			http.Error(client.W, err.Error(), http.StatusInternalServerError)
			return
		}

		// считаем новый баланс
		var newBalance float64
		if currentBalance.Valid {
			newBalance = currentBalance.Float64 + client.Amount
		} else {
			newBalance = client.Amount
		}

		// Обновляем баланс
		_, err = tx.Exec("UPDATE clients SET balance = $1 WHERE id = $2", newBalance, client.Id)
		if err != nil {
			tx.Rollback()
			http.Error(client.W, err.Error(), http.StatusInternalServerError)
			return
		}
		// коммит
		err = tx.Commit()
		if err != nil {
			http.Error(client.W, err.Error(), http.StatusInternalServerError)
			return
		}

	}

}
func handleWithdrawRequest(w http.ResponseWriter, r *http.Request) {
	// Добавляем запрос в очередь
	var a Amount
	_ = json.NewDecoder(r.Body).Decode(&a)
	if a.Amount <= 0 {
		http.Error(w, "insufficient funds", http.StatusBadRequest)
		return
	}
	//переводим id из
	clientID := r.FormValue("client_id")
	ID, err := strconv.Atoi(clientID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tx := WithdrawTransaction{
		Id:     ID,
		Amount: a.Amount,
		W:      w,
		R:      r,
	}
	if err := db.Ping(); err != nil {
		fmt.Println("Database is not available:", err)
		// write data to file if the database crashes
		writeFile(tx)
	}
	go func() {
		queueWithdraw <- tx
	}()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Withdrawal  accpeted"))

}

func withdrawProcess() {
	for _ = range queueWithdraw {

		client := <-queueWithdraw
		// Стартуем транзакцию
		tx, err := db.Begin()
		if err != nil {
			http.Error(client.W, err.Error(), http.StatusInternalServerError)
			return
		}

		// Получаем баланс
		var currentBalance sql.NullFloat64
		err = tx.QueryRow("SELECT balance FROM clients WHERE id = $1", client.Id).Scan(&currentBalance)
		if err != nil {
			tx.Rollback()
			http.Error(client.W, err.Error(), http.StatusInternalServerError)
			return
		}
		if !currentBalance.Valid {
			tx.Rollback()

			http.Error(client.W, "insufficient funds", http.StatusBadRequest)
			return
		}
		if currentBalance.Float64 < client.Amount {
			tx.Rollback()

			http.Error(client.W, "insufficient funds", http.StatusBadRequest)
			return
		}
		// считаем новый баланс
		var newBalance float64
		if currentBalance.Valid {
			newBalance = currentBalance.Float64 - client.Amount
		} else {
			tx.Rollback()
			http.Error(client.W, "insufficient funds", http.StatusBadRequest)
			return
		}

		// Обновляем баланс
		_, err = tx.Exec("UPDATE clients SET balance = $1 WHERE id = $2", newBalance, client.Id)
		if err != nil {
			tx.Rollback()
			http.Error(client.W, err.Error(), http.StatusInternalServerError)
			return
		}

		// коммит
		err = tx.Commit()
		if err != nil {
			http.Error(client.W, err.Error(), http.StatusInternalServerError)
			return
		}

	}
}

// для удобства
func GetClients(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var results []struct {
		Col1 int
		Col2 string
		Col3 sql.NullFloat64
	}
	rows, err := db.Query("SELECT * FROM clients")
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var result struct {
			Col1 int
			Col2 string
			Col3 sql.NullFloat64
		}
		err := rows.Scan(&result.Col1, &result.Col2, &result.Col3)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, result)
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	json.NewEncoder(w).Encode(results)
	// отправляем ответ клиенту
}

func CreateClient(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name") // Получаем имя из запроса
	var id int
	err := db.QueryRow("INSERT INTO clients (name) VALUES ($1) RETURNING id", name).Scan(&id)
	if err != nil {
		http.Error(w, "data base is down", http.StatusBadRequest)
	}

	fmt.Fprintf(w, "New client with ID %d and name %s created!", id, name)

}
func createTable() error {
	createClientsStatement := `
	CREATE TABLE IF NOT EXISTS clients (
		id SERIAL PRIMARY KEY,
		name VARCHAR(50) NOT NULL,
		balance NUMERIC(10, 2)
	);
`
	_, err := db.Exec(createClientsStatement)

	if err != nil {
		return err
	}
	return nil

}
func writeFile(transaction WithdrawTransaction) {
	// записываем траназкцию в файл если бд крашнулась
	file, err := os.OpenFile("database_crashed.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Failed to create file:", err)
		return
	}
	defer file.Close()

	type tx struct {
		time   int
		id     int
		amount float64
	}
	UserData := tx{time: int(time.Now().Unix()), id: transaction.Id, amount: transaction.Amount}
	defer file.Close()

	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = fmt.Fprintln(file, UserData)
	if err != nil {
		fmt.Println("Failed to write to file:", err)
		return
	}

}
