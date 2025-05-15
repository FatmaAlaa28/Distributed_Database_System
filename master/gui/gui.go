package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

// Client interacts with the master node via HTTP
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new HTTP client for the master node
func NewClient(baseURL string) *Client {
	return &Client{
		httpClient: &http.Client{},
		baseURL:    baseURL,
	}
}

// CreateDatabase sends a request to create a database
func (c *Client) CreateDatabase(dbName string) (string, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"db": dbName,
	})
	resp, err := c.httpClient.Post(c.baseURL+"/create-db", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

// CreateTable sends a request to create a table
func (c *Client) CreateTable(dbName, tableName string, columns string) (string, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"db":      dbName,
		"table":   tableName,
		"columns": strings.Split(columns, ","),
	})
	resp, err := c.httpClient.Post(c.baseURL+"/create-table", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

// InsertRecord sends a request to insert a record
func (c *Client) InsertRecord(dbName, tableName string, record string) (string, error) {
	recordMap := make(map[string]interface{})
	for _, pair := range strings.Split(record, ",") {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) == 2 {
			recordMap[parts[0]] = parts[1]
		}
	}
	reqBody, _ := json.Marshal(map[string]interface{}{
		"db":     dbName,
		"table":  tableName,
		"record": recordMap,
	})
	resp, err := c.httpClient.Post(c.baseURL+"/insert", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

// SelectRecords sends a request to retrieve records
func (c *Client) SelectRecords(dbName, tableName string) ([]map[string]interface{}, error) {
	u, _ := url.Parse(c.baseURL + "/select")
	q := u.Query()
	q.Set("table", tableName)
	if dbName != "" && dbName != "All Databases" {
		q.Set("db", dbName)
	}
	u.RawQuery = q.Encode()
	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var records []map[string]interface{}
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %v, raw: %s", err, string(body))
	}
	return records, nil
}

// UpdateRecord sends a request to update a record
func (c *Client) UpdateRecord(dbName, tableName string, id int, record string) (string, error) {
	recordMap := make(map[string]interface{})
	for _, pair := range strings.Split(record, ",") {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) == 2 {
			recordMap[parts[0]] = parts[1]
		}
	}
	reqBody, _ := json.Marshal(map[string]interface{}{
		"db":     dbName,
		"table":  tableName,
		"id":     id,
		"record": recordMap,
	})
	resp, err := c.httpClient.Post(c.baseURL+"/update", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

// DeleteRecord sends a request to delete a record
func (c *Client) DeleteRecord(dbName, tableName string, id int) (string, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"db":    dbName,
		"table": tableName,
		"id":    id,
	})
	resp, err := c.httpClient.Post(c.baseURL+"/delete", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

// DropTable sends a request to drop a table
func (c *Client) DropTable(dbName, tableName string) (string, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"db":    dbName,
		"table": tableName,
	})
	resp, err := c.httpClient.Post(c.baseURL+"/drop-table", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

// DropDatabase sends a request to drop the database
func (c *Client) DropDatabase(dbName string) (string, error) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"db": dbName,
	})
	resp, err := c.httpClient.Post(c.baseURL+"/drop-db", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

// GetDatabases sends a request to list databases
func (c *Client) GetDatabases() ([]string, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/databases")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var databases []string
	if err := json.Unmarshal(body, &databases); err != nil {
		return nil, err
	}
	return databases, nil
}

// GetTables sends a request to list tables
func (c *Client) GetTables(dbName string) ([]map[string]string, error) {
	u, _ := url.Parse(c.baseURL + "/tables")
	q := u.Query()
	if dbName != "" && dbName != "All Databases" {
		q.Set("db", dbName)
	}
	u.RawQuery = q.Encode()
	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var tables []map[string]string
	if err := json.Unmarshal(body, &tables); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %v, raw: %s", err, string(body))
	}
	return tables, nil
}

func main() {
	// Initialize Fyne app
	myApp := app.New()
	myWindow := myApp.NewWindow("Master Node GUI")
	myWindow.Resize(fyne.NewSize(800, 600))

	// Initialize client
	client := NewClient("http://localhost:8080")

	// Create tabs for different operations
	tabs := container.NewAppTabs(
		createDatabaseTab(client),
		createTableTab(client),
		insertRecordTab(client),
		selectRecordsTab(client),
		updateRecordTab(client),
		deleteRecordTab(client),
		dropTableTab(client),
		dropDatabaseTab(client),
		listDatabasesTab(client),
		listTablesTab(client),
	)

	// Set content and show window
	myWindow.SetContent(tabs)
	myWindow.ShowAndRun()
}

// refreshDatabases updates the database dropdown
func refreshDatabases(client *Client, dbSelect *widget.Select) {
	databases, err := client.GetDatabases()
	if err != nil {
		dbSelect.Options = []string{"Error: " + err.Error()}
	} else {
		dbSelect.Options = append([]string{"All Databases"}, databases...)
	}
	dbSelect.Refresh()
}

// refreshTables updates the table dropdown
func refreshTables(client *Client, dbName string, tableSelect *widget.Select) {
	if dbName == "All Databases" || dbName == "" {
		tableSelect.Options = []string{"Select a specific database to list tables"}
		tableSelect.Refresh()
		return
	}
	tables, err := client.GetTables(dbName)
	if err != nil {
		tableSelect.Options = []string{"Error: " + err.Error()}
		return
	}
	var tableOptions []string
	for _, table := range tables {
		if table["database"] == dbName {
			tableOptions = append(tableOptions, table["table"])
		}
	}
	if len(tableOptions) == 0 {
		tableOptions = []string{"No tables found"}
	}
	tableSelect.Options = tableOptions
	tableSelect.Refresh()
}

// refreshRecordIDs updates the ID dropdown
func refreshRecordIDs(client *Client, dbName, tableName string, idSelect *widget.Select) {
	if dbName == "All Databases" || dbName == "" || tableName == "" {
		idSelect.Options = []string{"Select a specific database and table"}
		idSelect.Refresh()
		return
	}
	records, err := client.SelectRecords(dbName, tableName)
	if err != nil {
		idSelect.Options = []string{"Error: " + err.Error()}
		idSelect.Refresh()
		return
	}
	var idOptions []string
	for _, record := range records {
		if id, ok := record["id"]; ok {
			switch v := id.(type) {
			case int:
				idOptions = append(idOptions, strconv.Itoa(v))
			case string:
				idOptions = append(idOptions, v)
			case float64:
				idOptions = append(idOptions, strconv.FormatFloat(v, 'f', 0, 64))
			default:
				idOptions = append(idOptions, fmt.Sprintf("%v", v))
			}
		}
	}
	if len(idOptions) == 0 {
		idOptions = []string{"No records found"}
	}
	idSelect.Options = idOptions
	idSelect.Refresh()
}

// createDatabaseTab creates the UI for creating databases
func createDatabaseTab(client *Client) *container.TabItem {
	dbNameEntry := widget.NewEntry()
	dbNameEntry.SetPlaceHolder("Enter database name")
	resultLabel := widget.NewLabel("")
	refreshButton := widget.NewButton("Refresh Databases", func() {
		refreshDatabases(client, widget.NewSelect([]string{}, func(string) {}))
	})

	submitButton := widget.NewButton("Create Database", func() {
		if dbNameEntry.Text == "" {
			resultLabel.SetText("Error: Database name is required")
			return
		}
		result, err := client.CreateDatabase(dbNameEntry.Text)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
		} else {
			resultLabel.SetText(result)
			refreshDatabases(client, widget.NewSelect([]string{}, func(string) {}))
		}
	})

	return container.NewTabItem("Create Database", container.NewVBox(
		widget.NewLabel("Create Database"),
		widget.NewLabel("Database Name"),
		dbNameEntry,
		refreshButton,
		submitButton,
		resultLabel,
	))
}

// createTableTab creates the UI for creating tables
func createTableTab(client *Client) *container.TabItem {
	dbSelect := widget.NewSelect([]string{}, func(string) {})
	refreshDatabases(client, dbSelect)
	refreshButton := widget.NewButton("Refresh Databases", func() {
		refreshDatabases(client, dbSelect)
	})
	tableNameEntry := widget.NewEntry()
	tableNameEntry.SetPlaceHolder("Enter table name")
	columnsEntry := widget.NewEntry()
	columnsEntry.SetPlaceHolder("Enter columns (comma-separated)")
	resultLabel := widget.NewLabel("")

	submitButton := widget.NewButton("Create Table", func() {
		if dbSelect.Selected == "" || dbSelect.Selected == "All Databases" || tableNameEntry.Text == "" || columnsEntry.Text == "" {
			resultLabel.SetText("Error: Select a specific database and fill all fields")
			return
		}
		result, err := client.CreateTable(dbSelect.Selected, tableNameEntry.Text, columnsEntry.Text)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
		} else {
			resultLabel.SetText(result)
			refreshDatabases(client, dbSelect)
		}
	})

	return container.NewTabItem("Create Table", container.NewVBox(
		widget.NewLabel("Create Table"),
		widget.NewLabel("Select Database"),
		dbSelect,
		refreshButton,
		widget.NewLabel("Table Name"),
		tableNameEntry,
		widget.NewLabel("Columns (comma-separated)"),
		columnsEntry,
		submitButton,
		resultLabel,
	))
}

// insertRecordTab creates the UI for inserting records
func insertRecordTab(client *Client) *container.TabItem {
	dbSelect := widget.NewSelect([]string{}, func(db string) {})
	refreshDatabases(client, dbSelect)
	tableSelect := widget.NewSelect([]string{}, func(string) {})
	refreshButton := widget.NewButton("Refresh", func() {
		refreshDatabases(client, dbSelect)
		if dbSelect.Selected != "" && dbSelect.Selected != "All Databases" {
			refreshTables(client, dbSelect.Selected, tableSelect)
		}
	})
	dbSelect.OnChanged = func(db string) {
		if db != "" && db != "All Databases" {
			refreshTables(client, db, tableSelect)
		} else {
			tableSelect.Options = []string{}
			tableSelect.Refresh()
		}
	}
	recordEntry := widget.NewEntry()
	recordEntry.SetPlaceHolder("Enter record (key:value,key:value)")
	resultLabel := widget.NewLabel("")

	submitButton := widget.NewButton("Insert Record", func() {
		if dbSelect.Selected == "" || dbSelect.Selected == "All Databases" || tableSelect.Selected == "" || recordEntry.Text == "" {
			resultLabel.SetText("Error: Select a specific database and table, and fill the record field")
			return
		}
		result, err := client.InsertRecord(dbSelect.Selected, tableSelect.Selected, recordEntry.Text)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
		} else {
			resultLabel.SetText(result)
			refreshTables(client, dbSelect.Selected, tableSelect)
		}
	})

	return container.NewTabItem("Insert Record", container.NewVBox(
		widget.NewLabel("Insert Record"),
		widget.NewLabel("Select Database"),
		dbSelect,
		widget.NewLabel("Select Table"),
		tableSelect,
		refreshButton,
		widget.NewLabel("Record (key:value,key:value)"),
		recordEntry,
		submitButton,
		resultLabel,
	))
}

// selectRecordsTab creates the UI for selecting records
func selectRecordsTab(client *Client) *container.TabItem {
	dbSelect := widget.NewSelect([]string{}, func(db string) {})
	refreshDatabases(client, dbSelect)
	tableSelect := widget.NewSelect([]string{}, func(string) {})
	refreshButton := widget.NewButton("Refresh", func() {
		refreshDatabases(client, dbSelect)
		if dbSelect.Selected != "" && dbSelect.Selected != "All Databases" {
			refreshTables(client, dbSelect.Selected, tableSelect)
		}
	})
	dbSelect.OnChanged = func(db string) {
		if db != "" && db != "All Databases" {
			refreshTables(client, db, tableSelect)
		} else {
			tableSelect.Options = []string{}
			tableSelect.Refresh()
		}
	}
	resultLabel := widget.NewLabel("")

	submitButton := widget.NewButton("Select Records", func() {
		if tableSelect.Selected == "" {
			resultLabel.SetText("Error: Table is required")
			return
		}
		records, err := client.SelectRecords(dbSelect.Selected, tableSelect.Selected)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
			return
		}
		resultText := ""
		if len(records) == 0 {
			resultLabel.SetText("No records found")
			return
		}
		for _, record := range records {
			for k, v := range record {
				resultText += fmt.Sprintf("%s: %v, ", k, v)
			}
			resultText += "\n"
		}
		resultLabel.SetText(resultText)
	})

	return container.NewTabItem("Select Records", container.NewVBox(
		widget.NewLabel("Select Records"),
		widget.NewLabel("Select Database (or All Databases)"),
		dbSelect,
		widget.NewLabel("Select Table"),
		tableSelect,
		refreshButton,
		submitButton,
		resultLabel,
	))
}

// updateRecordTab creates the UI for updating records
func updateRecordTab(client *Client) *container.TabItem {
	dbSelect := widget.NewSelect([]string{}, func(db string) {})
	refreshDatabases(client, dbSelect)
	tableSelect := widget.NewSelect([]string{}, func(string) {})
	idSelect := widget.NewSelect([]string{}, func(string) {})
	refreshButton := widget.NewButton("Refresh", func() {
		refreshDatabases(client, dbSelect)
		if dbSelect.Selected != "" && dbSelect.Selected != "All Databases" {
			refreshTables(client, dbSelect.Selected, tableSelect)
			if tableSelect.Selected != "" {
				refreshRecordIDs(client, dbSelect.Selected, tableSelect.Selected, idSelect)
			}
		}
	})
	dbSelect.OnChanged = func(db string) {
		if db != "" && db != "All Databases" {
			refreshTables(client, db, tableSelect)
			if tableSelect.Selected != "" {
				refreshRecordIDs(client, db, tableSelect.Selected, idSelect)
			}
		} else {
			tableSelect.Options = []string{}
			tableSelect.Refresh()
			idSelect.Options = []string{}
			idSelect.Refresh()
		}
	}
	tableSelect.OnChanged = func(table string) {
		if table != "" && dbSelect.Selected != "" && dbSelect.Selected != "All Databases" {
			refreshRecordIDs(client, dbSelect.Selected, table, idSelect)
		} else {
			idSelect.Options = []string{}
			idSelect.Refresh()
		}
	}
	recordEntry := widget.NewEntry()
	recordEntry.SetPlaceHolder("Enter record (key:value,key:value)")
	resultLabel := widget.NewLabel("")

	submitButton := widget.NewButton("Update Record", func() {
		if dbSelect.Selected == "" || dbSelect.Selected == "All Databases" || tableSelect.Selected == "" || idSelect.Selected == "" || recordEntry.Text == "" {
			resultLabel.SetText("Error: Select a specific database and table, and fill all fields")
			return
		}
		id, err := strconv.Atoi(idSelect.Selected)
		if err != nil {
			resultLabel.SetText("Error: Invalid ID")
			return
		}
		result, err := client.UpdateRecord(dbSelect.Selected, tableSelect.Selected, id, recordEntry.Text)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
		} else {
			resultLabel.SetText(result)
			refreshRecordIDs(client, dbSelect.Selected, tableSelect.Selected, idSelect)
		}
	})

	return container.NewTabItem("Update Record", container.NewVBox(
		widget.NewLabel("Update Record"),
		widget.NewLabel("Select Database"),
		dbSelect,
		widget.NewLabel("Select Table"),
		tableSelect,
		widget.NewLabel("Select ID"),
		idSelect,
		refreshButton,
		widget.NewLabel("Record (key:value,key:value)"),
		recordEntry,
		submitButton,
		resultLabel,
	))
}

// deleteRecordTab creates the UI for deleting records
func deleteRecordTab(client *Client) *container.TabItem {
	dbSelect := widget.NewSelect([]string{}, func(db string) {})
	refreshDatabases(client, dbSelect)
	tableSelect := widget.NewSelect([]string{}, func(string) {})
	idSelect := widget.NewSelect([]string{}, func(string) {})
	refreshButton := widget.NewButton("Refresh", func() {
		refreshDatabases(client, dbSelect)
		if dbSelect.Selected != "" && dbSelect.Selected != "All Databases" {
			refreshTables(client, dbSelect.Selected, tableSelect)
			if tableSelect.Selected != "" {
				refreshRecordIDs(client, dbSelect.Selected, tableSelect.Selected, idSelect)
			}
		}
	})
	dbSelect.OnChanged = func(db string) {
		if db != "" && db != "All Databases" {
			refreshTables(client, db, tableSelect)
			if tableSelect.Selected != "" {
				refreshRecordIDs(client, db, tableSelect.Selected, idSelect)
			}
		} else {
			tableSelect.Options = []string{}
			tableSelect.Refresh()
			idSelect.Options = []string{}
			idSelect.Refresh()
		}
	}
	tableSelect.OnChanged = func(table string) {
		if table != "" && dbSelect.Selected != "" && dbSelect.Selected != "All Databases" {
			refreshRecordIDs(client, dbSelect.Selected, table, idSelect)
		} else {
			idSelect.Options = []string{}
			idSelect.Refresh()
		}
	}
	resultLabel := widget.NewLabel("")

	submitButton := widget.NewButton("Delete Record", func() {
		if dbSelect.Selected == "" || dbSelect.Selected == "All Databases" || tableSelect.Selected == "" || idSelect.Selected == "" {
			resultLabel.SetText("Error: Select a specific database and table, and an ID")
			return
		}
		id, err := strconv.Atoi(idSelect.Selected)
		if err != nil {
			resultLabel.SetText("Error: Invalid ID")
			return
		}
		result, err := client.DeleteRecord(dbSelect.Selected, tableSelect.Selected, id)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
		} else {
			resultLabel.SetText(result)
			refreshRecordIDs(client, dbSelect.Selected, tableSelect.Selected, idSelect)
		}
	})

	return container.NewTabItem("Delete Record", container.NewVBox(
		widget.NewLabel("Delete Record"),
		widget.NewLabel("Select Database"),
		dbSelect,
		widget.NewLabel("Select Table"),
		tableSelect,
		widget.NewLabel("Select ID"),
		idSelect,
		refreshButton,
		submitButton,
		resultLabel,
	))
}

// dropTableTab creates the UI for dropping tables
func dropTableTab(client *Client) *container.TabItem {
	dbSelect := widget.NewSelect([]string{}, func(db string) {})
	refreshDatabases(client, dbSelect)
	tableSelect := widget.NewSelect([]string{}, func(string) {})
	refreshButton := widget.NewButton("Refresh", func() {
		refreshDatabases(client, dbSelect)
		if dbSelect.Selected != "" && dbSelect.Selected != "All Databases" {
			refreshTables(client, dbSelect.Selected, tableSelect)
		}
	})
	dbSelect.OnChanged = func(db string) {
		if db != "" && db != "All Databases" {
			refreshTables(client, db, tableSelect)
		} else {
			tableSelect.Options = []string{}
			tableSelect.Refresh()
		}
	}
	resultLabel := widget.NewLabel("")

	submitButton := widget.NewButton("Drop Table", func() {
		if dbSelect.Selected == "" || dbSelect.Selected == "All Databases" || tableSelect.Selected == "" {
			resultLabel.SetText("Error: Select a specific database and table")
			return
		}
		result, err := client.DropTable(dbSelect.Selected, tableSelect.Selected)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
		} else {
			resultLabel.SetText(result)
			refreshTables(client, dbSelect.Selected, tableSelect)
		}
	})

	return container.NewTabItem("Drop Table", container.NewVBox(
		widget.NewLabel("Drop Table"),
		widget.NewLabel("Select Database"),
		dbSelect,
		widget.NewLabel("Select Table"),
		tableSelect,
		refreshButton,
		submitButton,
		resultLabel,
	))
}

// dropDatabaseTab creates the UI for dropping databases
func dropDatabaseTab(client *Client) *container.TabItem {
	dbSelect := widget.NewSelect([]string{}, func(string) {})
	refreshDatabases(client, dbSelect)
	refreshButton := widget.NewButton("Refresh Databases", func() {
		refreshDatabases(client, dbSelect)
	})
	resultLabel := widget.NewLabel("")

	submitButton := widget.NewButton("Drop Database", func() {
		if dbSelect.Selected == "" || dbSelect.Selected == "All Databases" {
			resultLabel.SetText("Error: Select a specific database")
			return
		}
		result, err := client.DropDatabase(dbSelect.Selected)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
		} else {
			resultLabel.SetText(result)
			refreshDatabases(client, dbSelect)
		}
	})

	return container.NewTabItem("Drop Database", container.NewVBox(
		widget.NewLabel("Drop Database"),
		widget.NewLabel("Select Database"),
		dbSelect,
		refreshButton,
		submitButton,
		resultLabel,
	))
}

// listDatabasesTab creates the UI for listing databases
func listDatabasesTab(client *Client) *container.TabItem {
	resultLabel := widget.NewLabel("")
	refreshButton := widget.NewButton("List Databases", func() {
		databases, err := client.GetDatabases()
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
			return
		}
		if len(databases) == 0 {
			resultLabel.SetText("No databases found")
			return
		}
		resultLabel.SetText(strings.Join(databases, "\n"))
	})

	return container.NewTabItem("List Databases", container.NewVBox(
		widget.NewLabel("List Databases"),
		refreshButton,
		resultLabel,
	))
}

// listTablesTab creates the UI for listing tables
func listTablesTab(client *Client) *container.TabItem {
	dbSelect := widget.NewSelect([]string{}, func(string) {})
	refreshDatabases(client, dbSelect)
	refreshButton := widget.NewButton("List Tables", func() {
		refreshDatabases(client, dbSelect)
	})
	resultLabel := widget.NewLabel("")

	submitButton := widget.NewButton("Show Tables", func() {
		tables, err := client.GetTables(dbSelect.Selected)
		if err != nil {
			resultLabel.SetText(fmt.Sprintf("Error: %v", err))
			return
		}
		if len(tables) == 0 {
			resultLabel.SetText("No tables found")
			return
		}
		var tableList []string
		for _, table := range tables {
			dbName := table["database"]
			tableName := table["table"]
			tableList = append(tableList, fmt.Sprintf("Database: %s, Table: %s", dbName, tableName))
		}
		resultLabel.SetText(strings.Join(tableList, "\n"))
	})

	return container.NewTabItem("List Tables", container.NewVBox(
		widget.NewLabel("List Tables"),
		widget.NewLabel("Select Database (or All Databases)"),
		dbSelect,
		refreshButton,
		submitButton,
		resultLabel,
	))
}