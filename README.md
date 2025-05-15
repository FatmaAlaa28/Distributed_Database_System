# Distributed Database System

This project implements a distributed database system using Go (Golang). The system consists of a master node and multiple slave nodes that communicate over HTTP, supporting data replication and basic database operations. A client interface is provided via a console and a GUI built with Fyne.

## System Architecture

Below is the structure of the distributed database system:

```
+--------------------+
|    MASTER NODE     |
| - Write Access     |
| - Create/Drop DB   |
| - Replicate to     |
|   Slaves via HTTP  |
| - MySQL Connection |
+--------------------+
          ↓
+--------------------+
|      MySQL DB      |
| - Stores Data      |
| - Shared Across    |
|   Nodes            |
+--------------------+
  ↑             ↑
+--------+    +--------+
|SLAVE   |    |SLAVE   |
|NODE    |    |NODE    |
| - Read- |    | - Read- |
|   Only  |    |   Only  |
| - MySQL |    | - MySQL |
|   Conn. |    |   Conn. |
+--------+    +--------+
```

- **Master Node**: Handles write operations, creates and drops databases, and replicates changes to slave nodes using HTTP endpoints (e.g., `/create-db`, `/insert`, `/replicate`).
- **Slave Nodes**: Handle read-only queries and maintain synchronization with the master via replication, connecting to the same MySQL database.
- **MySQL Database**: Centralized storage for all nodes (a single instance is used for simplicity; in production, replicated instances would be ideal).

## Project Structure

- **main.go**: Implements the master and slave nodes, handling database operations, replication, and HTTP server functionality.
- **client.go**: Provides a client interface with both a console-based and GUI-based (Fyne) frontend to interact with the master node.
- **Dependencies**:
  - Go standard library
  - `github.com/go-sql-driver/mysql` for MySQL connectivity
  - `fyne.io/fyne/v2` for the GUI

## Prerequisites

- **Go**: Version 1.16 or higher
- **MySQL**: A running MySQL server (version 5.7 or higher)
- **Dependencies**:
  ```bash
  go get github.com/go-sql-driver/mysql
  go get fyne.io/fyne/v2
  ```

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd distributed-database
   ```

2. **Configure MySQL**:
   - Ensure MySQL is running on `127.0.0.1:3306` (or update the `db-host` flag).
   - Create a MySQL user with appropriate permissions:
     ```sql
     CREATE USER 'root'@'localhost' IDENTIFIED BY 'rootroot';
     GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;
     FLUSH PRIVILEGES;
     ```

3. **Run the Master Node**:
   ```bash
   go run main.go -port=8080 -master=true -peers="192.168.220.158:8081,192.168.220.159:8082" -db-host="127.0.0.1:3306" -db-user="root" -db-pass="rootroot"
   ```
   - Replace `peers` with the IP addresses and ports of slave nodes.
   - Adjust `db-host`, `db-user`, and `db-pass` as needed.

4. **Run Slave Nodes**:
   On separate machines or terminals:
   ```bash
   go run main.go -port=8081 -master=false -db-host="127.0.0.1:3306" -db-user="root" -db-pass="rootroot"
   go run main.go -port=8082 -master=false -db-host="127.0.0.1:3306" -db-user="root" -db-pass="rootroot"
   ```
   - Each slave node connects to the same MySQL instance or a replicated instance.

5. **Run the Client**:
   - **Console Interface**:
     ```bash
     go run client.go -master-addr="http://192.168.220.157:8080"
     ```
     Follow the prompts to select databases, tables, and perform queries.
   - **GUI Interface**:
     ```bash
     go run client.go -master-addr="http://192.168.220.157:8080"
     ```
     The GUI will launch, allowing you to select databases and tables, and perform operations via buttons.

## Usage Examples

### Console Client
1. Start the client:
   ```bash
   go run client.go -master-addr="http://192.168.220.157:8080"
   ```
2. Select a database by entering its number.
3. Select a table.
4. Choose an operation (e.g., Insert, Select, Update, Delete).
5. For Insert/Update, enter data in the format `column1=value1,column2=value2`.
6. For Delete/Update, provide the record ID.

### GUI Client
1. Launch the GUI:
   ```bash
   go run client.go -master-addr="http://192.168.220.157:8080"
   ```
2. Select a database from the dropdown.
3. Select a table.
4. Use buttons to:
   - **Insert**: Add column-value pairs and submit.
   - **Select**: View records in a table.
   - **Update**: Enter record ID and updated column-value pairs.
   - **Delete**: Enter record ID to delete.

### Example Operations
- **Create Database** (Master only):
  - Handled automatically when inserting into a new database.
- **Create Table**:
  - Send a POST request to `/create-table`:
    ```json
    {
      "db": "testdb",
      "table": "users",
      "columns": ["name", "email"]
    }
    ```
- **Insert Record**:
  - Via GUI: Select table, click "Insert", add `name=John,email=john@example.com`.
  - Via Console: Enter `name=John,email=john@example.com` when prompted.
- **Select Records**:
  - View all records in a table via GUI or console.
- **Drop Database** (Master only):
  - Send a POST request to `/drop-db`:
    ```json
    {
      "db": "testdb"
    }
    ```

## Features

- **Core**:
  - Master node creates databases and tables dynamically.
  - All nodes support queries: search, update, delete, select, insert.
  - Master-only: Drop database.
  - Data replication from master to slaves via HTTP.
- **Bonus**:
  - Basic fault tolerance: Slave nodes can serve read queries if the master is down.
  - GUI interface using Fyne for user-friendly interaction.

## Notes

- The system assumes a single MySQL instance for simplicity. In a production environment, each node should connect to a replicated MySQL instance.
- Replication is synchronous; ensure network reliability for consistent performance.
- The GUI requires a desktop environment to run.

## Troubleshooting

- **MySQL Connection Errors**: Verify MySQL is running and credentials are correct.
- **Replication Failures**: Check peer addresses and network connectivity.
- **GUI Issues**: Ensure Fyne dependencies are installed and a display server is available.

## Authors

- **Asmaa Abdel Nasser**
- **Fatma Alzhraa Alaa**
- **Hanaa Mahmoud**
- **Mariam Hassan**
- **Wafaa Mostafa**
- **Yomna Mohamed**
