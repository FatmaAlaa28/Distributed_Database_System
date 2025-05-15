// package main

// import (
// 	"bytes"
// 	"database/sql"
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	"io"
// 	"log"
// 	"net/http"
// 	"strings"
// 	"sync"
// 	"time"

// 	_ "github.com/go-sql-driver/mysql"
// )

// // Database manages MySQL connection
// type Database struct {
// 	DB       *sql.DB
// 	Host     string
// 	User     string
// 	Password string
// }

// // NewDatabase creates a connection to the MySQL server (without selecting a specific database)
// func NewDatabase(dbHost, dbUser, dbPass string) (*Database, error) {
// 	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", dbUser, dbPass, dbHost)
// 	db, err := sql.Open("mysql", dsn)
// 	if err != nil {
// 		return nil, fmt.Errorf("error connecting to MySQL: %v", err)
// 	}
// 	if err := db.Ping(); err != nil {
// 		db.Close()
// 		return nil, fmt.Errorf("error pinging MySQL: %v", err)
// 	}
// 	return &Database{
// 		DB:       db,
// 		Host:     dbHost,
// 		User:     dbUser,
// 		Password: dbPass,
// 	}, nil
// }

// // CreateDatabase creates a new database if it doesn't exist
// func (db *Database) CreateDatabase(dbName string) error {
// 	var dbCount int
// 	err := db.DB.QueryRow("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", dbName).Scan(&dbCount)
// 	if err != nil {
// 		return fmt.Errorf("error checking database existence: %v", err)
// 	}
// 	if dbCount == 0 {
// 		_, err = db.DB.Exec(fmt.Sprintf("CREATE DATABASE `%s`", dbName))
// 		if err != nil {
// 			return fmt.Errorf("error creating database %s: %v", dbName, err)
// 		}
// 		log.Printf("Created database %s", dbName)
// 	}
// 	return nil
// }

// // CreateTable creates a new table in the specified database
// func (db *Database) CreateTable(dbName, tableName string, columns []string) error {
// 	if dbName == "" {
// 		return fmt.Errorf("database name is required")
// 	}
// 	// Switch to the specified database
// 	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
// 	if err != nil {
// 		return fmt.Errorf("error switching to database %s: %v", dbName, err)
// 	}

// 	var columnDefs []string
// 	for _, col := range columns {
// 		columnDefs = append(columnDefs, fmt.Sprintf("`%s` VARCHAR(255)", col))
// 	}
// 	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (id INT AUTO_INCREMENT PRIMARY KEY, %s)", tableName, strings.Join(columnDefs, ", "))
// 	_, err = db.DB.Exec(query)
// 	return err
// }

// // InsertRecord inserts a record into a table in the specified database
// func (db *Database) InsertRecord(dbName, tableName string, record map[string]interface{}) error {
// 	if dbName == "" {
// 		return fmt.Errorf("database name is required")
// 	}
// 	// Switch to the specified database
// 	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
// 	if err != nil {
// 		return fmt.Errorf("error switching to database %s: %v", dbName, err)
// 	}

// 	columns := make([]string, 0, len(record))
// 	placeholders := make([]string, 0, len(record))
// 	values := make([]interface{}, 0, len(record))
// 	for col, val := range record {
// 		columns = append(columns, fmt.Sprintf("`%s`", col))
// 		placeholders = append(placeholders, "?")
// 		values = append(values, val)
// 	}
// 	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", tableName, strings.Join(columns, ","), strings.Join(placeholders, ","))
// 	_, err = db.DB.Exec(query, values...)
// 	return err
// }

// // SelectRecords retrieves all records from a table across all databases or a specific database
// func (db *Database) SelectRecords(tableName string, dbName string) ([]map[string]interface{}, error) {
// 	var databases []string
// 	if dbName == "" {
// 		// Get all databases if no specific database is provided
// 		rows, err := db.DB.Query("SHOW DATABASES")
// 		if err != nil {
// 			return nil, fmt.Errorf("error listing databases: %v", err)
// 		}
// 		defer rows.Close()

// 		for rows.Next() {
// 			var dbName string
// 			if err := rows.Scan(&dbName); err != nil {
// 				return nil, fmt.Errorf("error scanning database name: %v", err)
// 			}
// 			if dbName != "information_schema" && dbName != "mysql" && dbName != "performance_schema" && dbName != "sys" {
// 				databases = append(databases, dbName)
// 			}
// 		}
// 	} else {
// 		databases = []string{dbName}
// 	}

// 	var allRecords []map[string]interface{}
// 	for _, dbName := range databases {
// 		// Switch to the database
// 		_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
// 		if err != nil {
// 			log.Printf("Skipping database %s due to error: %v", dbName, err)
// 			continue
// 		}

// 		// Check if the table exists in this database
// 		var tableCount int
// 		err = db.DB.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", dbName, tableName).Scan(&tableCount)
// 		if err != nil {
// 			log.Printf("Error checking table %s in database %s: %v", tableName, dbName, err)
// 			continue
// 		}
// 		if tableCount == 0 {
// 			continue
// 		}

// 		// Query the table
// 		query := fmt.Sprintf("SELECT * FROM `%s`", tableName)
// 		rows, err := db.DB.Query(query)
// 		if err != nil {
// 			log.Printf("Error querying table %s in database %s: %v", tableName, dbName, err)
// 			continue
// 		}
// 		defer rows.Close()

// 		columns, err := rows.Columns()
// 		if err != nil {
// 			log.Printf("Error getting columns for table %s in database %s: %v", tableName, dbName, err)
// 			continue
// 		}

// 		for rows.Next() {
// 			values := make([]interface{}, len(columns))
// 			valuePtrs := make([]interface{}, len(columns))
// 			for i := range values {
// 				valuePtrs[i] = &values[i]
// 			}
// 			if err := rows.Scan(valuePtrs...); err != nil {
// 				log.Printf("Error scanning row from table %s in database %s: %v", tableName, dbName, err)
// 				continue
// 			}
// 			record := make(map[string]interface{})
// 			for i, col := range columns {
// 				val := values[i]
// 				if b, ok := val.([]byte); ok {
// 					record[col] = string(b)
// 				} else {
// 					record[col] = val
// 				}
// 				// Add database name to the record for clarity
// 				record["database"] = dbName
// 			}
// 			allRecords = append(allRecords, record)
// 		}
// 	}
// 	return allRecords, nil
// }

// // UpdateRecord updates a record in a table in the specified database
// func (db *Database) UpdateRecord(dbName, tableName string, id int, record map[string]interface{}) error {
// 	if dbName == "" {
// 		return fmt.Errorf("database name is required")
// 	}
// 	// Switch to the specified database
// 	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
// 	if err != nil {
// 		return fmt.Errorf("error switching to database %s: %v", dbName, err)
// 	}

// 	setClause := ""
// 	values := []interface{}{}
// 	for col, val := range record {
// 		if setClause != "" {
// 			setClause += ", "
// 		}
// 		setClause += fmt.Sprintf("`%s` = ?", col)
// 		values = append(values, val)
// 	}
// 	values = append(values, id)
// 	query := fmt.Sprintf("UPDATE `%s` SET %s WHERE id = ?", tableName, setClause)
// 	_, err = db.DB.Exec(query, values...)
// 	return err
// }

// // DeleteRecord deletes a record from a table in the specified database
// func (db *Database) DeleteRecord(dbName, tableName string, id int) error {
// 	if dbName == "" {
// 		return fmt.Errorf("database name is required")
// 	}
// 	// Switch to the specified database
// 	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
// 	if err != nil {
// 		return fmt.Errorf("error switching to database %s: %v", dbName, err)
// 	}

// 	query := fmt.Sprintf("DELETE FROM `%s` WHERE id = ?", tableName)
// 	_, err = db.DB.Exec(query, id)
// 	return err
// }

// // DropTable drops a table from the specified database
// func (db *Database) DropTable(dbName, tableName string) error {
// 	if dbName == "" {
// 		return fmt.Errorf("database name is required")
// 	}
// 	// Switch to the specified database
// 	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
// 	if err != nil {
// 		return fmt.Errorf("error switching to database %s: %v", dbName, err)
// 	}

// 	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
// 	_, err = db.DB.Exec(query)
// 	return err
// }

// // DropDatabase drops the specified database
// func (db *Database) DropDatabase(dbName string) error {
// 	if dbName == "" {
// 		return fmt.Errorf("database name is required")
// 	}
// 	query := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName)
// 	_, err := db.DB.Exec(query)
// 	return err
// }

// // GetDatabases retrieves all databases
// func (db *Database) GetDatabases() ([]string, error) {
// 	rows, err := db.DB.Query("SHOW DATABASES")
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	var databases []string
// 	for rows.Next() {
// 		var dbName string
// 		if err := rows.Scan(&dbName); err != nil {
// 			return nil, err
// 		}
// 		if dbName != "information_schema" && dbName != "mysql" && dbName != "performance_schema" && dbName != "sys" {
// 			databases = append(databases, dbName)
// 		}
// 	}
// 	return databases, nil
// }

// // GetTables retrieves all tables in the specified database or all databases
// func (db *Database) GetTables(dbName string) ([]map[string]string, error) {
// 	var databases []string
// 	if dbName == "" {
// 		var err error
// 		databases, err = db.GetDatabases()
// 		if err != nil {
// 			return nil, fmt.Errorf("error listing databases: %v", err)
// 		}
// 	} else {
// 		databases = []string{dbName}
// 	}

// 	var allTables []map[string]string
// 	for _, dbName := range databases {
// 		// Switch to the database
// 		_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
// 		if err != nil {
// 			log.Printf("Skipping database %s due to error: %v", dbName, err)
// 			continue
// 		}

// 		rows, err := db.DB.Query("SHOW TABLES")
// 		if err != nil {
// 			log.Printf("Error listing tables in database %s: %v", dbName, err)
// 			continue
// 		}
// 		defer rows.Close()

// 		for rows.Next() {
// 			var tableName string
// 			if err := rows.Scan(&tableName); err != nil {
// 				log.Printf("Error scanning table in database %s: %v", dbName, err)
// 				continue
// 			}
// 			allTables = append(allTables, map[string]string{
// 				"database": dbName,
// 				"table":    tableName,
// 			})
// 		}
// 	}
// 	return allTables, nil
// }

// // Close closes the database connection
// func (db *Database) Close() {
// 	db.DB.Close()
// }

// // Node represents the Node
// type Node struct {
// 	ID       string
// 	IsMaster bool
// 	DB       *Database
// 	Peers    []string
// 	mu       sync.Mutex
// 	client   *http.Client
// }

// // NewNode creates a new node
// func NewNode(id, dbHost, dbUser, dbPass string, isMaster bool, peers []string) (*Node, error) {
// 	db, err := NewDatabase(dbHost, dbUser, dbPass)
// 	if err != nil {
// 		return nil, err
// 	}
// 	// Validate peer addresses
// 	validPeers := make([]string, 0, len(peers))
// 	for _, peer := range peers {
// 		if strings.Contains(peer, ":") && !strings.HasPrefix(peer, ":") && !strings.HasSuffix(peer, ":") {
// 			validPeers = append(validPeers, peer)
// 		}
// 	}
// 	return &Node{
// 		ID:       id,
// 		IsMaster: isMaster,
// 		DB:       db,
// 		Peers:    validPeers,
// 		client: &http.Client{
// 			Timeout: 5 * time.Second,
// 		},
// 	}, nil
// }

// // ReplicateToPeers sends a request to all peers
// func (n *Node) ReplicateToPeers(endpoint string, payload interface{}) error {
// 	if !n.IsMaster {
// 		return fmt.Errorf("only master can replicate")
// 	}

// 	var wg sync.WaitGroup
// 	errChan := make(chan error, len(n.Peers))

// 	for _, peer := range n.Peers {
// 		wg.Add(1)
// 		go func(peer string) {
// 			defer wg.Done()
// 			url := fmt.Sprintf("http://%s%s", peer, endpoint)
// 			data, err := json.Marshal(payload)
// 			if err != nil {
// 				errChan <- fmt.Errorf("error marshaling payload for peer %s: %v", peer, err)
// 				return
// 			}

// 			resp, err := n.client.Post(url, "application/json", bytes.NewBuffer(data))
// 			if err != nil {
// 				errChan <- fmt.Errorf("error replicating to peer %s: %v", peer, err)
// 				return
// 			}
// 			defer resp.Body.Close()

// 			if resp.StatusCode != http.StatusOK {
// 				errChan <- fmt.Errorf("peer %s responded with status %d", peer, resp.StatusCode)
// 			}
// 		}(peer)
// 	}

// 	wg.Wait()
// 	close(errChan)

// 	for err := range errChan {
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// // CreateTable handles table creation
// func (n *Node) CreateTable(dbName, tableName string, columns []string) error {
// 	n.mu.Lock()
// 	defer n.mu.Unlock()

// 	err := n.DB.CreateTable(dbName, tableName, columns)
// 	if err != nil {
// 		return err
// 	}

// 	if n.IsMaster {
// 		err = n.ReplicateToPeers("/create-table", map[string]interface{}{
// 			"db":      dbName,
// 			"table":   tableName,
// 			"columns": columns,
// 		})
// 		if err != nil {
// 			log.Printf("Replication error: %v", err)
// 		}
// 	}
// 	return nil
// }

// // InsertRecord handles record insertion
// func (n *Node) InsertRecord(dbName, tableName string, record map[string]interface{}) error {
// 	n.mu.Lock()
// 	defer n.mu.Unlock()

// 	err := n.DB.InsertRecord(dbName, tableName, record)
// 	if err != nil {
// 		return err
// 	}

// 	if n.IsMaster {
// 		err = n.ReplicateToPeers("/insert", map[string]interface{}{
// 			"db":     dbName,
// 			"table":  tableName,
// 			"record": record,
// 		})
// 		if err != nil {
// 			log.Printf("Replication error: %v", err)
// 		}
// 	}
// 	return nil
// }

// // SelectRecords retrieves records from the database
// func (n *Node) SelectRecords(tableName, dbName string) ([]map[string]interface{}, error) {
// 	n.mu.Lock()
// 	defer n.mu.Unlock()
// 	return n.DB.SelectRecords(tableName, dbName)
// }

// // UpdateRecord handles record updates
// func (n *Node) UpdateRecord(dbName, tableName string, id int, record map[string]interface{}) error {
// 	n.mu.Lock()
// 	defer n.mu.Unlock()

// 	err := n.DB.UpdateRecord(dbName, tableName, id, record)
// 	if err != nil {
// 		return err
// 	}

// 	if n.IsMaster {
// 		err = n.ReplicateToPeers("/update", map[string]interface{}{
// 			"db":     dbName,
// 			"table":  tableName,
// 			"id":     id,
// 			"record": record,
// 		})
// 		if err != nil {
// 			log.Printf("Replication error: %v", err)
// 		}
// 	}
// 	return nil
// }

// // DeleteRecord handles record deletion
// func (n *Node) DeleteRecord(dbName, tableName string, id int) error {
// 	n.mu.Lock()
// 	defer n.mu.Unlock()

// 	err := n.DB.DeleteRecord(dbName, tableName, id)
// 	if err != nil {
// 		return err
// 	}

// 	if n.IsMaster {
// 		err = n.ReplicateToPeers("/delete", map[string]interface{}{
// 			"db":    dbName,
// 			"table": tableName,
// 			"id":    id,
// 		})
// 		if err != nil {
// 			log.Printf("Replication error: %v", err)
// 		}
// 	}
// 	return nil
// }

// // DropTable handles table dropping
// func (n *Node) DropTable(dbName, tableName string) error {
// 	n.mu.Lock()
// 	defer n.mu.Unlock()

// 	err := n.DB.DropTable(dbName, tableName)
// 	if err != nil {
// 		return err
// 	}

// 	if n.IsMaster {
// 		err = n.ReplicateToPeers("/drop-table", map[string]interface{}{
// 			"db":    dbName,
// 			"table": tableName,
// 		})
// 		if err != nil {
// 			log.Printf("Replication error: %v", err)
// 		}
// 	}
// 	return nil
// }

// // DropDatabase handles database dropping
// func (n *Node) DropDatabase(dbName string) error {
// 	if !n.IsMaster {
// 		return fmt.Errorf("only master can drop database")
// 	}

// 	n.mu.Lock()
// 	defer n.mu.Unlock()
// 	return n.DB.DropDatabase(dbName)
// }

// // GetDatabases retrieves all databases
// func (n *Node) GetDatabases() ([]string, error) {
// 	n.mu.Lock()
// 	defer n.mu.Unlock()
// 	return n.DB.GetDatabases()
// }

// // GetTables retrieves all tables in the specified database or all databases
// func (n *Node) GetTables(dbName string) ([]map[string]string, error) {
// 	n.mu.Lock()
// 	defer n.mu.Unlock()
// 	return n.DB.GetTables(dbName)
// }

// // Close closes the node
// func (n *Node) Close() {
// 	n.DB.Close()
// }

// // IsAlive checks if the node is alive
// func (n *Node) IsAlive() bool {
// 	return n.DB.DB.Ping() == nil
// }

// // MonitorPeers periodically checks peer health
// func (n *Node) MonitorPeers(errorLog io.Writer) {
// 	if !n.IsMaster {
// 		return
// 	}

// 	ticker := time.NewTicker(10 * time.Second)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		for _, peer := range n.Peers {
// 			url := fmt.Sprintf("http://%s/heartbeat", peer)
// 			resp, err := n.client.Get(url)
// 			if err != nil {
// 				fmt.Fprintf(errorLog, "Peer %s is down: %v\n", peer, err)
// 				continue
// 			}
// 			resp.Body.Close()
// 			if resp.StatusCode != http.StatusOK {
// 				fmt.Fprintf(errorLog, "Peer %s responded with status %d\n", peer, resp.StatusCode)
// 			}
// 		}
// 	}
// }

// // Server provides an HTTP interface
// type Server struct {
// 	Node *Node
// }

// // NewServer creates a new server
// func NewServer(node *Node) *Server {
// 	return &Server{Node: node}
// }

// // Start starts the HTTP server
// func (s *Server) Start(port string) {
// 	if s.Node.IsMaster {
// 		go s.Node.MonitorPeers(io.Discard) // Suppress peer monitoring logs
// 	}

// 	http.HandleFunc("/create-table", s.createTableHandler)
// 	http.HandleFunc("/insert", s.insertHandler)
// 	http.HandleFunc("/select", s.selectHandler)
// 	http.HandleFunc("/update", s.updateHandler)
// 	http.HandleFunc("/delete", s.deleteHandler)
// 	http.HandleFunc("/drop-table", s.dropTableHandler)
// 	http.HandleFunc("/drop-db", s.dropDBHandler)
// 	http.HandleFunc("/databases", s.databasesHandler)
// 	http.HandleFunc("/tables", s.tablesHandler)
// 	http.HandleFunc("/heartbeat", s.heartbeatHandler)

// 	log.Printf("Starting node on port %s", port)
// 	err := http.ListenAndServe("0.0.0.0:"+port, nil)
// 	if err != nil {
// 		log.Fatalf("Server failed: %v", err)
// 	}
// }

// // createTableHandler handles table creation requests
// func (s *Server) createTableHandler(w http.ResponseWriter, r *http.Request) {
// 	var req struct {
// 		DB      string   `json:"db"`
// 		Table   string   `json:"table"`
// 		Columns []string `json:"columns"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	if req.DB == "" {
// 		http.Error(w, "db parameter is required", http.StatusBadRequest)
// 		return
// 	}
// 	// Ensure the database exists
// 	if err := s.Node.DB.CreateDatabase(req.DB); err != nil {
// 		http.Error(w, fmt.Sprintf("error creating database %s: %v", req.DB, err), http.StatusInternalServerError)
// 		return
// 	}
// 	if err := s.Node.CreateTable(req.DB, req.Table, req.Columns); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s created table: %s in database: %s with columns: %v", r.RemoteAddr, req.Table, req.DB, req.Columns)
// 	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
// }

// // insertHandler handles record insertion requests
// func (s *Server) insertHandler(w http.ResponseWriter, r *http.Request) {
// 	var req struct {
// 		DB     string                 `json:"db"`
// 		Table  string                 `json:"table"`
// 		Record map[string]interface{} `json:"record"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	if req.DB == "" {
// 		http.Error(w, "db parameter is required", http.StatusBadRequest)
// 		return
// 	}
// 	// Ensure the database exists
// 	if err := s.Node.DB.CreateDatabase(req.DB); err != nil {
// 		http.Error(w, fmt.Sprintf("error creating database %s: %v", req.DB, err), http.StatusInternalServerError)
// 		return
// 	}
// 	if err := s.Node.InsertRecord(req.DB, req.Table, req.Record); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s inserted record into table: %s in database: %s, record: %v", r.RemoteAddr, req.Table, req.DB, req.Record)
// 	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
// }

// // selectHandler handles record retrieval requests
// func (s *Server) selectHandler(w http.ResponseWriter, r *http.Request) {
// 	table := r.URL.Query().Get("table")
// 	dbName := r.URL.Query().Get("db")
// 	records, err := s.Node.SelectRecords(table, dbName)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s retrieved records from table: %s (db: %s)", r.RemoteAddr, table, dbName)
// 	json.NewEncoder(w).Encode(records)
// }

// // updateHandler handles record update requests
// func (s *Server) updateHandler(w http.ResponseWriter, r *http.Request) {
// 	var req struct {
// 		DB     string                 `json:"db"`
// 		Table  string                 `json:"table"`
// 		ID     int                    `json:"id"`
// 		Record map[string]interface{} `json:"record"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	if req.DB == "" {
// 		http.Error(w, "db parameter is required", http.StatusBadRequest)
// 		return
// 	}
// 	if err := s.Node.UpdateRecord(req.DB, req.Table, req.ID, req.Record); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s updated record in table: %s in database: %s, ID: %d, record: %v", r.RemoteAddr, req.Table, req.DB, req.ID, req.Record)
// 	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
// }

// // deleteHandler handles record deletion requests
// func (s *Server) deleteHandler(w http.ResponseWriter, r *http.Request) {
// 	var req struct {
// 		DB    string `json:"db"`
// 		Table string `json:"table"`
// 		ID    int    `json:"id"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	if req.DB == "" {
// 		http.Error(w, "db parameter is required", http.StatusBadRequest)
// 		return
// 	}
// 	if err := s.Node.DeleteRecord(req.DB, req.Table, req.ID); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s deleted record from table: %s in database: %s, ID: %d", r.RemoteAddr, req.Table, req.DB, req.ID)
// 	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
// }

// // dropTableHandler handles table drop requests
// func (s *Server) dropTableHandler(w http.ResponseWriter, r *http.Request) {
// 	var req struct {
// 		DB    string `json:"db"`
// 		Table string `json:"table"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	if req.DB == "" {
// 		http.Error(w, "db parameter is required", http.StatusBadRequest)
// 		return
// 	}
// 	if err := s.Node.DropTable(req.DB, req.Table); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s dropped table: %s in database: %s", r.RemoteAddr, req.Table, req.DB)
// 	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
// }

// // dropDBHandler handles database drop requests
// func (s *Server) dropDBHandler(w http.ResponseWriter, r *http.Request) {
// 	if !s.Node.IsMaster {
// 		http.Error(w, "only master can drop database", http.StatusForbidden)
// 		return
// 	}
// 	var req struct {
// 		DB string `json:"db"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}
// 	if req.DB == "" {
// 		http.Error(w, "db parameter is required", http.StatusBadRequest)
// 		return
// 	}
// 	if err := s.Node.DropDatabase(req.DB); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s dropped database: %s", r.RemoteAddr, req.DB)
// 	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
// }

// // databasesHandler returns the list of databases
// func (s *Server) databasesHandler(w http.ResponseWriter, r *http.Request) {
// 	databases, err := s.Node.GetDatabases()
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s retrieved list of databases", r.RemoteAddr)
// 	json.NewEncoder(w).Encode(databases)
// }

// // tablesHandler returns the list of tables in the specified database or all databases
// func (s *Server) tablesHandler(w http.ResponseWriter, r *http.Request) {
// 	dbName := r.URL.Query().Get("db")
// 	tables, err := s.Node.GetTables(dbName)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	log.Printf("Client %s retrieved list of tables for database: %s", r.RemoteAddr, dbName)
// 	json.NewEncoder(w).Encode(tables)
// }

// // heartbeatHandler checks if the node is alive
// func (s *Server) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
// 	if s.Node.IsAlive() {
// 		json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
// 	} else {
// 		http.Error(w, "Node is down", http.StatusServiceUnavailable)
// 	}
// }

// func main() {
// 	port := flag.String("port", "8080", "Port to run the node")
// 	isMaster := flag.Bool("master", true, "Run as master node")
// 	peers := flag.String("peers", "", "Comma-separated list of peer addresses")
// 	dbHost := flag.String("db-host", "127.0.0.1:3306", "MySQL host")
// 	dbUser := flag.String("db-user", "root", "MySQL user")
// 	dbPass := flag.String("db-pass", "rootroot", "MySQL password")
// 	flag.Parse()

// 	peerList := []string{}
// 	if *peers != "" {
// 		peerList = strings.Split(*peers, ",")
// 	}

// 	n, err := NewNode("node-"+*port, *dbHost, *dbUser, *dbPass, *isMaster, peerList)
// 	if err != nil {
// 		log.Fatalf("Failed to create node: %v", err)
// 	}
// 	defer n.Close()

// 	server := NewServer(n)
// 	server.Start(*port)
// }

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Database manages MySQL connection
type Database struct {
	DB       *sql.DB
	Host     string
	User     string
	Password string
}

// NewDatabase creates a connection to the MySQL server (without selecting a specific database)
func NewDatabase(dbHost, dbUser, dbPass string) (*Database, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", dbUser, dbPass, dbHost)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error connecting to MySQL: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging MySQL: %v", err)
	}
	return &Database{
		DB:       db,
		Host:     dbHost,
		User:     dbUser,
		Password: dbPass,
	}, nil
}

// CreateDatabase creates a new database if it doesn't exist
func (db *Database) CreateDatabase(dbName string) error {
	var dbCount int
	err := db.DB.QueryRow("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", dbName).Scan(&dbCount)
	if err != nil {
		return fmt.Errorf("error checking database existence: %v", err)
	}
	if dbCount == 0 {
		_, err = db.DB.Exec(fmt.Sprintf("CREATE DATABASE `%s`", dbName))
		if err != nil {
			return fmt.Errorf("error creating database %s: %v", dbName, err)
		}
		log.Printf("Created database %s", dbName)
	}
	return nil
}

// CreateTable creates a new table in the specified database
func (db *Database) CreateTable(dbName, tableName string, columns []string) error {
	if dbName == "" {
		return fmt.Errorf("database name is required")
	}
	// Switch to the specified database
	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
	if err != nil {
		return fmt.Errorf("error switching to database %s: %v", dbName, err)
	}

	var columnDefs []string
	for _, col := range columns {
		columnDefs = append(columnDefs, fmt.Sprintf("`%s` VARCHAR(255)", col))
	}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (id INT AUTO_INCREMENT PRIMARY KEY, %s)", tableName, strings.Join(columnDefs, ", "))
	_, err = db.DB.Exec(query)
	return err
}

// InsertRecord inserts a record into a table in the specified database
func (db *Database) InsertRecord(dbName, tableName string, record map[string]interface{}) error {
	if dbName == "" {
		return fmt.Errorf("database name is required")
	}
	// Switch to the specified database
	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
	if err != nil {
		return fmt.Errorf("error switching to database %s: %v", dbName, err)
	}

	columns := make([]string, 0, len(record))
	placeholders := make([]string, 0, len(record))
	values := make([]interface{}, 0, len(record))
	for col, val := range record {
		columns = append(columns, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}
	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", tableName, strings.Join(columns, ","), strings.Join(placeholders, ","))
	_, err = db.DB.Exec(query, values...)
	return err
}

// SelectRecords retrieves all records from a table across all databases or a specific database
func (db *Database) SelectRecords(tableName string, dbName string) ([]map[string]interface{}, error) {
	var databases []string
	if dbName == "" {
		// Get all databases if no specific database is provided
		rows, err := db.DB.Query("SHOW DATABASES")
		if err != nil {
			return nil, fmt.Errorf("error listing databases: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var dbName string
			if err := rows.Scan(&dbName); err != nil {
				return nil, fmt.Errorf("error scanning database name: %v", err)
			}
			if dbName != "information_schema" && dbName != "mysql" && dbName != "performance_schema" && dbName != "sys" {
				databases = append(databases, dbName)
			}
		}
	} else {
		databases = []string{dbName}
	}

	var allRecords []map[string]interface{}
	for _, dbName := range databases {
		// Switch to the database
		_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
		if err != nil {
			log.Printf("Skipping database %s due to error: %v", dbName, err)
			continue
		}

		// Check if the table exists in this database
		var tableCount int
		err = db.DB.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", dbName, tableName).Scan(&tableCount)
		if err != nil {
			log.Printf("Error checking table %s in database %s: %v", tableName, dbName, err)
			continue
		}
		if tableCount == 0 {
			continue
		}

		// Query the table
		query := fmt.Sprintf("SELECT * FROM `%s`", tableName)
		rows, err := db.DB.Query(query)
		if err != nil {
			log.Printf("Error querying table %s in database %s: %v", tableName, dbName, err)
			continue
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			log.Printf("Error getting columns for table %s in database %s: %v", tableName, dbName, err)
			continue
		}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}
			if err := rows.Scan(valuePtrs...); err != nil {
				log.Printf("Error scanning row from table %s in database %s: %v", tableName, dbName, err)
				continue
			}
			record := make(map[string]interface{})
			for i, col := range columns {
				val := values[i]
				if b, ok := val.([]byte); ok {
					record[col] = string(b)
				} else {
					record[col] = val
				}
				// Add database name to the record for clarity
				record["database"] = dbName
			}
			allRecords = append(allRecords, record)
		}
	}
	return allRecords, nil
}

// UpdateRecord updates a record in a table in the specified database
func (db *Database) UpdateRecord(dbName, tableName string, id int, record map[string]interface{}) error {
	if dbName == "" {
		return fmt.Errorf("database name is required")
	}
	// Switch to the specified database
	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
	if err != nil {
		return fmt.Errorf("error switching to database %s: %v", dbName, err)
	}

	setClause := ""
	values := []interface{}{}
	for col, val := range record {
		if setClause != "" {
			setClause += ", "
		}
		setClause += fmt.Sprintf("`%s` = ?", col)
		values = append(values, val)
	}
	values = append(values, id)
	query := fmt.Sprintf("UPDATE `%s` SET %s WHERE id = ?", tableName, setClause)
	_, err = db.DB.Exec(query, values...)
	return err
}

// DeleteRecord deletes a record from a table in the specified database
func (db *Database) DeleteRecord(dbName, tableName string, id int) error {
	if dbName == "" {
		return fmt.Errorf("database name is required")
	}
	// Switch to the specified database
	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
	if err != nil {
		return fmt.Errorf("error switching to database %s: %v", dbName, err)
	}

	query := fmt.Sprintf("DELETE FROM `%s` WHERE id = ?", tableName)
	_, err = db.DB.Exec(query, id)
	return err
}

// DropTable drops a table from the specified database
func (db *Database) DropTable(dbName, tableName string) error {
	if dbName == "" {
		return fmt.Errorf("database name is required")
	}
	// Switch to the specified database
	_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
	if err != nil {
		return fmt.Errorf("error switching to database %s: %v", dbName, err)
	}

	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
	_, err = db.DB.Exec(query)
	return err
}

// DropDatabase drops the specified database
func (db *Database) DropDatabase(dbName string) error {
	if dbName == "" {
		return fmt.Errorf("database name is required")
	}
	query := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName)
	_, err := db.DB.Exec(query)
	return err
}

// GetDatabases retrieves all databases
func (db *Database) GetDatabases() ([]string, error) {
	rows, err := db.DB.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}
		if dbName != "information_schema" && dbName != "mysql" && dbName != "performance_schema" && dbName != "sys" {
			databases = append(databases, dbName)
		}
	}
	return databases, nil
}

// GetTables retrieves all tables in the specified database or all databases
func (db *Database) GetTables(dbName string) ([]map[string]string, error) {
	var databases []string
	if dbName == "" {
		var err error
		databases, err = db.GetDatabases()
		if err != nil {
			return nil, fmt.Errorf("error listing databases: %v", err)
		}
	} else {
		databases = []string{dbName}
	}

	var allTables []map[string]string
	for _, dbName := range databases {
		// Switch to the database
		_, err := db.DB.Exec(fmt.Sprintf("USE `%s`", dbName))
		if err != nil {
			log.Printf("Skipping database %s due to error: %v", dbName, err)
			continue
		}

		rows, err := db.DB.Query("SHOW TABLES")
		if err != nil {
			log.Printf("Error listing tables in database %s: %v", dbName, err)
			continue
		}
		defer rows.Close()

		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				log.Printf("Error scanning table in database %s: %v", dbName, err)
				continue
			}
			allTables = append(allTables, map[string]string{
				"database": dbName,
				"table":    tableName,
			})
		}
	}
	return allTables, nil
}

// Close closes the database connection
func (db *Database) Close() {
	db.DB.Close()
}

// Node represents the Node
type Node struct {
	ID       string
	IsMaster bool
	DB       *Database
	Peers    []string
	mu       sync.Mutex
	client   *http.Client
}

// NewNode creates a new node
func NewNode(id, dbHost, dbUser, dbPass string, isMaster bool, peers []string) (*Node, error) {
	db, err := NewDatabase(dbHost, dbUser, dbPass)
	if err != nil {
		return nil, err
	}
	// Validate peer addresses
	validPeers := make([]string, 0, len(peers))
	for _, peer := range peers {
		if strings.Contains(peer, ":") && !strings.HasPrefix(peer, ":") && !strings.HasSuffix(peer, ":") {
			validPeers = append(validPeers, peer)
		}
	}
	return &Node{
		ID:       id,
		IsMaster: isMaster,
		DB:       db,
		Peers:    validPeers,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}, nil
}

// CreateDatabase handles database creation
func (n *Node) CreateDatabase(dbName string) error {
	if !n.IsMaster {
		return fmt.Errorf("only master can create database")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	err := n.DB.CreateDatabase(dbName)
	if err != nil {
		return err
	}

	err = n.ReplicateToPeers("/create-db", map[string]interface{}{
		"db": dbName,
	})
	if err != nil {
		log.Printf("Replication error: %v", err)
	}
	return nil
}

// CreateTable handles table creation
func (n *Node) CreateTable(dbName, tableName string, columns []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	err := n.DB.CreateTable(dbName, tableName, columns)
	if err != nil {
		return err
	}

	if n.IsMaster {
		err = n.ReplicateToPeers("/create-table", map[string]interface{}{
			"db":      dbName,
			"table":   tableName,
			"columns": columns,
		})
		if err != nil {
			log.Printf("Replication error: %v", err)
		}
	}
	return nil
}

// InsertRecord handles record insertion
func (n *Node) InsertRecord(dbName, tableName string, record map[string]interface{}) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	err := n.DB.InsertRecord(dbName, tableName, record)
	if err != nil {
		return err
	}

	if n.IsMaster {
		err = n.ReplicateToPeers("/insert", map[string]interface{}{
			"db":     dbName,
			"table":  tableName,
			"record": record,
		})
		if err != nil {
			log.Printf("Replication error: %v", err)
		}
	}
	return nil
}

// SelectRecords retrieves records from the database
func (n *Node) SelectRecords(tableName, dbName string) ([]map[string]interface{}, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.DB.SelectRecords(tableName, dbName)
}

// UpdateRecord handles record updates
func (n *Node) UpdateRecord(dbName, tableName string, id int, record map[string]interface{}) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	err := n.DB.UpdateRecord(dbName, tableName, id, record)
	if err != nil {
		return err
	}

	if n.IsMaster {
		err = n.ReplicateToPeers("/update", map[string]interface{}{
			"db":     dbName,
			"table":  tableName,
			"id":     id,
			"record": record,
		})
		if err != nil {
			log.Printf("Replication error: %v", err)
		}
	}
	return nil
}

// DeleteRecord handles record deletion
func (n *Node) DeleteRecord(dbName, tableName string, id int) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	err := n.DB.DeleteRecord(dbName, tableName, id)
	if err != nil {
		return err
	}

	if n.IsMaster {
		err = n.ReplicateToPeers("/delete", map[string]interface{}{
			"db":    dbName,
			"table": tableName,
			"id":    id,
		})
		if err != nil {
			log.Printf("Replication error: %v", err)
		}
	}
	return nil
}

// DropTable handles table dropping
func (n *Node) DropTable(dbName, tableName string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	err := n.DB.DropTable(dbName, tableName)
	if err != nil {
		return err
	}

	if n.IsMaster {
		err = n.ReplicateToPeers("/drop-table", map[string]interface{}{
			"db":    dbName,
			"table": tableName,
		})
		if err != nil {
			log.Printf("Replication error: %v", err)
		}
	}
	return nil
}

// DropDatabase handles database dropping
func (n *Node) DropDatabase(dbName string) error {
	if !n.IsMaster {
		return fmt.Errorf("only master can drop database")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	return n.DB.DropDatabase(dbName)
}

// GetDatabases retrieves all databases
func (n *Node) GetDatabases() ([]string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.DB.GetDatabases()
}

// GetTables retrieves all tables in the specified database or all databases
func (n *Node) GetTables(dbName string) ([]map[string]string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.DB.GetTables(dbName)
}

// Close closes the node
func (n *Node) Close() {
	n.DB.Close()
}

// IsAlive checks if the node is alive
func (n *Node) IsAlive() bool {
	return n.DB.DB.Ping() == nil
}

// ReplicateToPeers sends a request to all peers
func (n *Node) ReplicateToPeers(endpoint string, payload interface{}) error {
	if !n.IsMaster {
		return fmt.Errorf("only master can replicate")
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(n.Peers))

	for _, peer := range n.Peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s%s", peer, endpoint)
			data, err := json.Marshal(payload)
			if err != nil {
				errChan <- fmt.Errorf("error marshaling payload for peer %s: %v", peer, err)
				return
			}

			resp, err := n.client.Post(url, "application/json", bytes.NewBuffer(data))
			if err != nil {
				errChan <- fmt.Errorf("error replicating to peer %s: %v", peer, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errChan <- fmt.Errorf("peer %s responded with status %d", peer, resp.StatusCode)
			}
		}(peer)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

// MonitorPeers periodically checks peer health
func (n *Node) MonitorPeers(errorLog io.Writer) {
	if !n.IsMaster {
		return
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, peer := range n.Peers {
			url := fmt.Sprintf("http://%s/heartbeat", peer)
			resp, err := n.client.Get(url)
			if err != nil {
				fmt.Fprintf(errorLog, "Peer %s is down: %v\n", peer, err)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				fmt.Fprintf(errorLog, "Peer %s responded with status %d\n", peer, resp.StatusCode)
			}
		}
	}
}

// Server provides an HTTP interface
type Server struct {
	Node *Node
}

// NewServer creates a new server
func NewServer(node *Node) *Server {
	return &Server{Node: node}
}

// Start starts the HTTP server
func (s *Server) Start(port string) {
	if s.Node.IsMaster {
		go s.Node.MonitorPeers(io.Discard) // Suppress peer monitoring logs
	}

	http.HandleFunc("/create-db", s.createDBHandler)
	http.HandleFunc("/create-table", s.createTableHandler)
	http.HandleFunc("/insert", s.insertHandler)
	http.HandleFunc("/select", s.selectHandler)
	http.HandleFunc("/update", s.updateHandler)
	http.HandleFunc("/delete", s.deleteHandler)
	http.HandleFunc("/drop-table", s.dropTableHandler)
	http.HandleFunc("/drop-db", s.dropDBHandler)
	http.HandleFunc("/databases", s.databasesHandler)
	http.HandleFunc("/tables", s.tablesHandler)
	http.HandleFunc("/heartbeat", s.heartbeatHandler)

	log.Printf("Starting node on port %s", port)
	err := http.ListenAndServe("0.0.0.0:"+port, nil)
	if err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// createDBHandler handles database creation requests
func (s *Server) createDBHandler(w http.ResponseWriter, r *http.Request) {
	if !s.Node.IsMaster {
		http.Error(w, "only master can create database", http.StatusForbidden)
		return
	}
	var req struct {
		DB string `json:"db"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DB == "" {
		http.Error(w, "db parameter is required", http.StatusBadRequest)
		return
	}
	if err := s.Node.CreateDatabase(req.DB); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s created database: %s", r.RemoteAddr, req.DB)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// createTableHandler handles table creation requests
func (s *Server) createTableHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB      string   `json:"db"`
		Table   string   `json:"table"`
		Columns []string `json:"columns"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DB == "" {
		http.Error(w, "db parameter is required", http.StatusBadRequest)
		return
	}
	// Ensure the database exists
	if err := s.Node.DB.CreateDatabase(req.DB); err != nil {
		http.Error(w, fmt.Sprintf("error creating database %s: %v", req.DB, err), http.StatusInternalServerError)
		return
	}
	if err := s.Node.CreateTable(req.DB, req.Table, req.Columns); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s created table: %s in database: %s with columns: %v", r.RemoteAddr, req.Table, req.DB, req.Columns)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// insertHandler handles record insertion requests
func (s *Server) insertHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string                 `json:"db"`
		Table  string                 `json:"table"`
		Record map[string]interface{} `json:"record"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DB == "" {
		http.Error(w, "db parameter is required", http.StatusBadRequest)
		return
	}
	// Ensure the database exists
	if err := s.Node.DB.CreateDatabase(req.DB); err != nil {
		http.Error(w, fmt.Sprintf("error creating database %s: %v", req.DB, err), http.StatusInternalServerError)
		return
	}
	if err := s.Node.InsertRecord(req.DB, req.Table, req.Record); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s inserted record into table: %s in database: %s, record: %v", r.RemoteAddr, req.Table, req.DB, req.Record)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// selectHandler handles record retrieval requests
func (s *Server) selectHandler(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	dbName := r.URL.Query().Get("db")
	records, err := s.Node.SelectRecords(table, dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s retrieved records from table: %s (db: %s)", r.RemoteAddr, table, dbName)
	json.NewEncoder(w).Encode(records)
}

// updateHandler handles record update requests
func (s *Server) updateHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB     string                 `json:"db"`
		Table  string                 `json:"table"`
		ID     int                    `json:"id"`
		Record map[string]interface{} `json:"record"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DB == "" {
		http.Error(w, "db parameter is required", http.StatusBadRequest)
		return
	}
	if err := s.Node.UpdateRecord(req.DB, req.Table, req.ID, req.Record); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s updated record in table: %s in database: %s, ID: %d, record: %v", r.RemoteAddr, req.Table, req.DB, req.ID, req.Record)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// deleteHandler handles record deletion requests
func (s *Server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
		ID    int    `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DB == "" {
		http.Error(w, "db parameter is required", http.StatusBadRequest)
		return
	}
	if err := s.Node.DeleteRecord(req.DB, req.Table, req.ID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s deleted record from table: %s in database: %s, ID: %d", r.RemoteAddr, req.Table, req.DB, req.ID)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// dropTableHandler handles table drop requests
func (s *Server) dropTableHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DB    string `json:"db"`
		Table string `json:"table"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DB == "" {
		http.Error(w, "db parameter is required", http.StatusBadRequest)
		return
	}
	if err := s.Node.DropTable(req.DB, req.Table); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s dropped table: %s in database: %s", r.RemoteAddr, req.Table, req.DB)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// dropDBHandler handles database drop requests
func (s *Server) dropDBHandler(w http.ResponseWriter, r *http.Request) {
	if !s.Node.IsMaster {
		http.Error(w, "only master can drop database", http.StatusForbidden)
		return
	}
	var req struct {
		DB string `json:"db"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.DB == "" {
		http.Error(w, "db parameter is required", http.StatusBadRequest)
		return
	}
	if err := s.Node.DropDatabase(req.DB); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s dropped database: %s", r.RemoteAddr, req.DB)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// databasesHandler returns the list of databases
func (s *Server) databasesHandler(w http.ResponseWriter, r *http.Request) {
	databases, err := s.Node.GetDatabases()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s retrieved list of databases", r.RemoteAddr)
	json.NewEncoder(w).Encode(databases)
}

// tablesHandler returns the list of tables in the specified database or all databases
func (s *Server) tablesHandler(w http.ResponseWriter, r *http.Request) {
	dbName := r.URL.Query().Get("db")
	tables, err := s.Node.GetTables(dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Client %s retrieved list of tables for database: %s", r.RemoteAddr, dbName)
	json.NewEncoder(w).Encode(tables)
}

// heartbeatHandler checks if the node is alive
func (s *Server) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	if s.Node.IsAlive() {
		json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
	} else {
		http.Error(w, "Node is down", http.StatusServiceUnavailable)
	}
}

func main() {
	port := flag.String("port", "8080", "Port to run the node")
	isMaster := flag.Bool("master", true, "Run as master node")
	peers := flag.String("peers", "", "Comma-separated list of peer addresses")
	dbHost := flag.String("db-host", "127.0.0.1:3306", "MySQL host")
	dbUser := flag.String("db-user", "root", "MySQL user")
	dbPass := flag.String("db-pass", "rootroot", "MySQL password")
	flag.Parse()

	peerList := []string{}
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	n, err := NewNode("node-"+*port, *dbHost, *dbUser, *dbPass, *isMaster, peerList)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer n.Close()

	server := NewServer(n)
	server.Start(*port)
}
