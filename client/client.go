package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// Client interacts with Master Node
type Client struct {
	MasterAddr string
}

// NewClient creates a new client
func NewClient(masterAddr string) *Client {
	return &Client{MasterAddr: masterAddr}
}

// GetDatabases retrieves all databases from Master
func (c *Client) GetDatabases() ([]string, error) {
	url := c.MasterAddr + "/databases"
	log.Printf("Requesting: %s", url)
	for retries := 3; retries > 0; retries-- {
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Failed to fetch databases: %v, retrying...", err)
			time.Sleep(time.Second)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			headers := make(map[string]string)
			for k, v := range resp.Header {
				headers[k] = strings.Join(v, ", ")
			}
			headerStr, _ := json.MarshalIndent(headers, "", "  ")
			log.Printf("Unexpected status code %d for %s: %s\nHeaders: %s", resp.StatusCode, url, string(body), string(headerStr))
			return nil, fmt.Errorf("unexpected status code %d for %s: %s", resp.StatusCode, url, string(body))
		}

		var databases []string
		if err := json.NewDecoder(resp.Body).Decode(&databases); err != nil {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("failed to decode databases JSON from %s: %v, response: %s", url, err, string(body))
		}
		return databases, nil
	}
	return nil, fmt.Errorf("failed to fetch databases after retries")
}

// GetTables retrieves all tables in a database from Master
// GetTables retrieves all tables in a database from Master
func (c *Client) GetTables(dbName string) ([]string, error) {
    url := c.MasterAddr + "/tables?db=" + dbName
    log.Printf("Requesting: %s", url)
    resp, err := http.Get(url)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch tables: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        headers := make(map[string]string)
        for k, v := range resp.Header {
            headers[k] = strings.Join(v, ", ")
        }
        headerStr, _ := json.MarshalIndent(headers, "", "  ")
        log.Printf("Unexpected status code %d for %s: %s\nHeaders: %s", resp.StatusCode, url, string(body), string(headerStr))
        return nil, fmt.Errorf("unexpected status code %d for %s: %s", resp.StatusCode, url, string(body))
    }

    var tablesResp []map[string]string
    if err := json.NewDecoder(resp.Body).Decode(&tablesResp); err != nil {
        body, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("failed to decode tables JSON from %s: %v, response: %s", url, err, string(body))
    }

    // Extract table names from the response
    var tables []string
    for _, tableInfo := range tablesResp {
        tables = append(tables, tableInfo["table"])
    }
    return tables, nil
}

// InsertRecord sends an insert request to Master
func (c *Client) InsertRecord(tableName string, record map[string]interface{}) error {
	url := c.MasterAddr + "/insert"
	log.Printf("Requesting: %s", url)
	payload, err := json.Marshal(map[string]interface{}{
		"table":  tableName,
		"record": record,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal insert payload: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to insert record: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		headers := make(map[string]string)
		for k, v := range resp.Header {
			headers[k] = strings.Join(v, ", ")
		}
		headerStr, _ := json.MarshalIndent(headers, "", "  ")
		log.Printf("Unexpected status code %d for %s: %s\nHeaders: %s", resp.StatusCode, url, string(body), string(headerStr))
		return fmt.Errorf("unexpected status code %d for %s: %s", resp.StatusCode, url, string(body))
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to decode insert response JSON from %s: %v, response: %s", url, err, string(body))
	}
	if result["status"] != "success" {
		return fmt.Errorf("insert failed: %s", result["error"])
	}
	return nil
}

// SelectRecords sends a select request to Master
func (c *Client) SelectRecords(tableName string) ([]map[string]interface{}, error) {
	url := c.MasterAddr + "/select?table=" + tableName
	log.Printf("Requesting: %s", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch records: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		headers := make(map[string]string)
		for k, v := range resp.Header {
			headers[k] = strings.Join(v, ", ")
		}
		headerStr, _ := json.MarshalIndent(headers, "", "  ")
		log.Printf("Unexpected status code %d for %s: %s\nHeaders: %s", resp.StatusCode, url, string(body), string(headerStr))
		return nil, fmt.Errorf("unexpected status code %d for %s: %s", resp.StatusCode, url, string(body))
	}

	var records []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to decode records JSON from %s: %v, response: %s", url, err, string(body))
	}
	return records, nil
}

// UpdateRecord sends an update request to Master
func (c *Client) UpdateRecord(dbName, tableName string, id int, record map[string]interface{}) error {
    url := c.MasterAddr + "/update"
    log.Printf("Requesting: %s", url)
    payload, err := json.Marshal(map[string]interface{}{
        "db":     dbName, // Add db field
        "table":  tableName,
        "id":     id,
        "record": record,
    })
    if err != nil {
        return fmt.Errorf("failed to marshal update payload: %v", err)
    }
    resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
    if err != nil {
        return fmt.Errorf("failed to update record: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        headers := make(map[string]string)
        for k, v := range resp.Header {
            headers[k] = strings.Join(v, ", ")
        }
        headerStr, _ := json.MarshalIndent(headers, "", "  ")
        log.Printf("Unexpected status code %d for %s: %s\nHeaders: %s", resp.StatusCode, url, string(body), string(headerStr))
        return fmt.Errorf("unexpected status code %d for %s: %s", resp.StatusCode, url, string(body))
    }

    var result map[string]string
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        body, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("failed to decode update response JSON from %s: %v, response: %s", url, err, string(body))
    }
    if result["status"] != "success" {
        return fmt.Errorf("update failed: %s", result["error"])
    }
    return nil
}

// DeleteRecord sends a delete request to Master
func (c *Client) DeleteRecord(dbName, tableName string, id int) error {
    url := c.MasterAddr + "/delete"
    log.Printf("Requesting: %s", url)
    payload, err := json.Marshal(map[string]interface{}{
        "db":    dbName, // Add db field
        "table": tableName,
        "id":    id,
    })
    if err != nil {
        return fmt.Errorf("failed to marshal delete payload: %v", err)
    }
    resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
    if err != nil {
        return fmt.Errorf("failed to delete record: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        headers := make(map[string]string)
        for k, v := range resp.Header {
            headers[k] = strings.Join(v, ", ")
        }
        headerStr, _ := json.MarshalIndent(headers, "", "  ")
        log.Printf("Unexpected status code %d for %s: %s\nHeaders: %s", resp.StatusCode, url, string(body), string(headerStr))
        return fmt.Errorf("unexpected status code %d for %s: %s", resp.StatusCode, url, string(body))
    }

    var result map[string]string
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        body, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("failed to decode delete response JSON from %s: %v, response: %s", url, err, string(body))
    }
    if result["status"] != "success" {
        return fmt.Errorf("delete failed: %s", result["error"])
    }
    return nil
}

// GUIInterface provides a graphical user interface
func (c *Client) GUIInterface() {
	myApp := app.NewWithID("com.example.dbclient")
	myApp.Settings().SetTheme(theme.DarkTheme()) // Use dark theme for modern look
	myWindow := myApp.NewWindow("Database Client")
	myWindow.Resize(fyne.NewSize(800, 600))

	// Status label for feedback
	statusLabel := widget.NewLabel("Ready")
	statusLabel.Alignment = fyne.TextAlignCenter

	// Table selection
	var tableSelect *widget.Select
	tableSelect = widget.NewSelect([]string{}, func(selected string) {
		if selected != "" {
			statusLabel.SetText("Selected table: " + selected)
		}
	})
	tableSelect.PlaceHolder = "Select a table..."

	// Database selection
	dbLabel := widget.NewLabel("Select Database:")
	var dbSelect *widget.Select
	dbSelect = widget.NewSelect([]string{}, func(selected string) {
		if selected == "" {
			return
		}
		tables, err := c.GetTables(selected)
		if err != nil {
			dialog.ShowError(err, myWindow)
			statusLabel.SetText("Error: " + err.Error())
			return
		}
		tableSelect.Options = tables
		tableSelect.Refresh()
		statusLabel.SetText("Loaded tables for " + selected)
	})
	dbSelect.PlaceHolder = "Select a database..."

	// Table label
	tableLabel := widget.NewLabel("Select Table:")

	// Operation buttons
	insertBtn := widget.NewButtonWithIcon("Insert Record", theme.ContentAddIcon(), func() {
		if tableSelect.Selected == "" {
			dialog.ShowError(fmt.Errorf("please select a table"), myWindow)
			statusLabel.SetText("Error: Please select a table")
			return
		}
		// Dialog with dynamic fields
		entries := make([]*widget.Entry, 0)
		items := make([]*widget.FormItem, 0)
		keyEntry := widget.NewEntry()
		keyEntry.SetPlaceHolder("Column name")
		valueEntry := widget.NewEntry()
		valueEntry.SetPlaceHolder("Value")
		entries = append(entries, keyEntry, valueEntry)
		items = append(items,
			&widget.FormItem{Text: "Column", Widget: keyEntry},
			&widget.FormItem{Text: "Value", Widget: valueEntry},
		)
		addFieldBtn := widget.NewButton("Add Field", func() {
			newKey := widget.NewEntry()
			newKey.SetPlaceHolder("Column name")
			newValue := widget.NewEntry()
			newValue.SetPlaceHolder("Value")
			entries = append(entries, newKey, newValue)
			items = append(items,
				&widget.FormItem{Text: "Column", Widget: newKey},
				&widget.FormItem{Text: "Value", Widget: newValue},
			)
			// Re-show dialog with updated fields
			dialog.ShowForm("Insert Record", "Insert", "Cancel", items, func(confirm bool) {
				if !confirm {
					return
				}
				record := make(map[string]interface{})
				for i := 0; i < len(entries); i += 2 {
					if entries[i].Text != "" && entries[i+1].Text != "" {
						record[entries[i].Text] = entries[i+1].Text
					}
				}
				if len(record) == 0 {
					dialog.ShowError(fmt.Errorf("no valid fields provided"), myWindow)
					statusLabel.SetText("Error: No valid fields provided")
					return
				}
				if err := c.InsertRecord(tableSelect.Selected, record); err != nil {
					dialog.ShowError(err, myWindow)
					statusLabel.SetText("Error: " + err.Error())
				} else {
					dialog.ShowInformation("Success", "Record inserted successfully", myWindow)
					statusLabel.SetText("Record inserted successfully")
				}
			}, myWindow)
		})
		items = append(items, &widget.FormItem{Text: "", Widget: addFieldBtn})
		dialog.ShowForm("Insert Record", "Insert", "Cancel", items, func(confirm bool) {
			if !confirm {
				return
			}
			record := make(map[string]interface{})
			for i := 0; i < len(entries); i += 2 {
				if entries[i].Text != "" && entries[i+1].Text != "" {
					record[entries[i].Text] = entries[i+1].Text
				}
			}
			if len(record) == 0 {
				dialog.ShowError(fmt.Errorf("no valid fields provided"), myWindow)
				statusLabel.SetText("Error: No valid fields provided")
				return
			}
			if err := c.InsertRecord(tableSelect.Selected, record); err != nil {
				dialog.ShowError(err, myWindow)
				statusLabel.SetText("Error: " + err.Error())
			} else {
				dialog.ShowInformation("Success", "Record inserted successfully", myWindow)
				statusLabel.SetText("Record inserted successfully")
			}
		}, myWindow)
	})

	selectBtn := widget.NewButtonWithIcon("Select Records", theme.ViewFullScreenIcon(), func() {
		if tableSelect.Selected == "" {
			dialog.ShowError(fmt.Errorf("please select a table"), myWindow)
			statusLabel.SetText("Error: Please select a table")
			return
		}
		records, err := c.SelectRecords(tableSelect.Selected)
		if err != nil {
			dialog.ShowError(err, myWindow)
			statusLabel.SetText("Error: " + err.Error())
			return
		}
		// Display records in a table
		recordsWindow := myApp.NewWindow("Records in " + tableSelect.Selected)
		recordsWindow.Resize(fyne.NewSize(600, 400))
		if len(records) == 0 {
			recordsWindow.SetContent(widget.NewLabel("No records found"))
			recordsWindow.Show()
			return
		}
		// Create table headers dynamically
		keys := make([]string, 0)
		for k := range records[0] {
			keys = append(keys, k)
		}
		table := widget.NewTable(
			func() (int, int) { return len(records) + 1, len(keys) }, // +1 for header
			func() fyne.CanvasObject {
				return widget.NewLabel("")
			},
			func(id widget.TableCellID, cell fyne.CanvasObject) {
				label := cell.(*widget.Label)
				if id.Row == 0 {
					label.SetText(keys[id.Col])
				} else {
					record := records[id.Row-1]
					label.SetText(fmt.Sprintf("%v", record[keys[id.Col]]))
				}
			},
		)
		for i := 0; i < len(keys); i++ {
			table.SetColumnWidth(i, 150)
		}
		recordsWindow.SetContent(container.NewScroll(table))
		recordsWindow.Show()
	})

	updateBtn := widget.NewButtonWithIcon("Update Record", theme.DocumentSaveIcon(), func() {
		if tableSelect.Selected == "" {
			dialog.ShowError(fmt.Errorf("please select a table"), myWindow)
			statusLabel.SetText("Error: Please select a table")
			return
		}
		if dbSelect.Selected == "" {
			dialog.ShowError(fmt.Errorf("please select a database"), myWindow)
			statusLabel.SetText("Error: Please select a database")
			return
		}
		idEntry := widget.NewEntry()
		idEntry.SetPlaceHolder("Record ID")
		entries := make([]*widget.Entry, 0)
		items := []*widget.FormItem{
			{Text: "ID", Widget: idEntry},
		}
		keyEntry := widget.NewEntry()
		keyEntry.SetPlaceHolder("Column name")
		valueEntry := widget.NewEntry()
		valueEntry.SetPlaceHolder("Value")
		entries = append(entries, keyEntry, valueEntry)
		items = append(items,
			&widget.FormItem{Text: "Column", Widget: keyEntry},
			&widget.FormItem{Text: "Value", Widget: valueEntry},
		)
		addFieldBtn := widget.NewButton("Add Field", func() {
			newKey := widget.NewEntry()
			newKey.SetPlaceHolder("Column name")
			newValue := widget.NewEntry()
			newValue.SetPlaceHolder("Value")
			entries = append(entries, newKey, newValue)
			items = append(items,
				&widget.FormItem{Text: "Column", Widget: newKey},
				&widget.FormItem{Text: "Value", Widget: newValue},
			)
			dialog.ShowForm("Update Record", "Update", "Cancel", items, func(confirm bool) {
				if !confirm {
					return
				}
				id, err := strconv.Atoi(idEntry.Text)
				if err != nil {
					dialog.ShowError(fmt.Errorf("invalid ID"), myWindow)
					statusLabel.SetText("Error: Invalid ID")
					return
				}
				record := make(map[string]interface{})
				for i := 0; i < len(entries); i += 2 {
					if entries[i].Text != "" && entries[i+1].Text != "" {
						record[entries[i].Text] = entries[i+1].Text
					}
				}
				if len(record) == 0 {
					dialog.ShowError(fmt.Errorf("no valid fields provided"), myWindow)
					statusLabel.SetText("Error: No valid fields provided")
					return
				}
				// Pass dbSelect.Selected as the dbName
				if err := c.UpdateRecord(dbSelect.Selected, tableSelect.Selected, id, record); err != nil {
					dialog.ShowError(err, myWindow)
					statusLabel.SetText("Error: " + err.Error())
				} else {
					dialog.ShowInformation("Success", "Record updated successfully", myWindow)
					statusLabel.SetText("Record updated successfully")
				}
			}, myWindow)
		})
		items = append(items, &widget.FormItem{Text: "", Widget: addFieldBtn})
		dialog.ShowForm("Update Record", "Update", "Cancel", items, func(confirm bool) {
			if !confirm {
				return
			}
			id, err := strconv.Atoi(idEntry.Text)
			if err != nil {
				dialog.ShowError(fmt.Errorf("invalid ID"), myWindow)
				statusLabel.SetText("Error: Invalid ID")
				return
			}
			record := make(map[string]interface{})
			for i := 0; i < len(entries); i += 2 {
				if entries[i].Text != "" && entries[i+1].Text != "" {
					record[entries[i].Text] = entries[i+1].Text
				}
			}
			if len(record) == 0 {
				dialog.ShowError(fmt.Errorf("no valid fields provided"), myWindow)
				statusLabel.SetText("Error: No valid fields provided")
				return
			}
			// Pass dbSelect.Selected as the dbName
			if err := c.UpdateRecord(dbSelect.Selected, tableSelect.Selected, id, record); err != nil {
				dialog.ShowError(err, myWindow)
				statusLabel.SetText("Error: " + err.Error())
			} else {
				dialog.ShowInformation("Success", "Record updated successfully", myWindow)
				statusLabel.SetText("Record updated successfully")
			}
		}, myWindow)
	})

	deleteBtn := widget.NewButtonWithIcon("Delete Record", theme.DeleteIcon(), func() {
		if tableSelect.Selected == "" {
			dialog.ShowError(fmt.Errorf("please select a table"), myWindow)
			statusLabel.SetText("Error: Please select a table")
			return
		}
		if dbSelect.Selected == "" {
			dialog.ShowError(fmt.Errorf("please select a database"), myWindow)
			statusLabel.SetText("Error: Please select a database")
			return
		}
		idEntry := widget.NewEntry()
		idEntry.SetPlaceHolder("Record ID")
		dialog.ShowForm("Delete Record", "Delete", "Cancel",
			[]*widget.FormItem{{Text: "ID", Widget: idEntry}},
			func(confirm bool) {
				if !confirm {
					return
				}
				id, err := strconv.Atoi(idEntry.Text)
				if err != nil {
					dialog.ShowError(fmt.Errorf("invalid ID"), myWindow)
					statusLabel.SetText("Error: Invalid ID")
					return
				}
				// Pass dbSelect.Selected as the dbName
				if err := c.DeleteRecord(dbSelect.Selected, tableSelect.Selected, id); err != nil {
					dialog.ShowError(err, myWindow)
					statusLabel.SetText("Error: " + err.Error())
				} else {
					dialog.ShowInformation("Success", "Record deleted successfully", myWindow)
					statusLabel.SetText("Record deleted successfully")
				}
			}, myWindow)
	})

	// Layout (simplified to match second code's vertical structure, but with split layout)
	buttons := container.NewVBox(insertBtn, selectBtn, updateBtn, deleteBtn)
	sidebar := container.NewVBox(
		dbLabel, dbSelect,
		tableLabel, tableSelect,
		widget.NewLabel("Operations:"),
		buttons,
	)
	mainContent := container.NewCenter(widget.NewLabel("Select a table to view records"))
	split := container.NewHSplit(sidebar, mainContent)
	split.Offset = 0.3 // Sidebar takes 30% of width
	content := container.NewVBox(
		split,
		layout.NewSpacer(),
		statusLabel,
	)
	myWindow.SetContent(content)

	// Load databases on start
	go func() {
		databases, err := c.GetDatabases()
		if err != nil {
			dialog.ShowError(err, myWindow)
			return
		}
		dbSelect.Options = databases
		dbSelect.Refresh()
	}()

	myWindow.ShowAndRun()
}

func main() {
	masterAddr := flag.String("master-addr", "http://192.168.43.97:8080", "Master node address")
	flag.Parse()

	client := NewClient(*masterAddr)
	client.GUIInterface()
}