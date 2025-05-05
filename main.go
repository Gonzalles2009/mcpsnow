package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	_ "github.com/snowflakedb/gosnowflake"
)

func main() {
	log.Println("MCP Snowflake Server Starting...")

	// Load Snowflake connection settings from environment
	account := os.Getenv("SNOWFLAKE_ACCOUNT")
	user := os.Getenv("SNOWFLAKE_USER")
	password := os.Getenv("SNOWFLAKE_PASSWORD") // Keep password local, don't log it
	database := os.Getenv("SNOWFLAKE_DATABASE")
	schema := os.Getenv("SNOWFLAKE_SCHEMA")
	warehouse := os.Getenv("SNOWFLAKE_WAREHOUSE")
	role := os.Getenv("SNOWFLAKE_ROLE") // optional

	log.Printf("Read ENV VARS: ACCOUNT=%s, USER=%s, DATABASE=%s, SCHEMA=%s, WAREHOUSE=%s, ROLE=%s", account, user, database, schema, warehouse, role)

	if account == "" || user == "" || password == "" || database == "" || schema == "" || warehouse == "" {
		log.Fatal("FATAL: Missing one or more required SNOWFLAKE_* environment variables")
	}

	// Build DSN for gosnowflake
	dsn := fmt.Sprintf("%s:***@%s/%s/%s?warehouse=%s", user, account, database, schema, warehouse) // Log DSN without password
	if role != "" {
		dsn += fmt.Sprintf("&role=%s", role)
	}
	log.Printf("Built DSN (password masked): %s", dsn)

	// Rebuild DSN with password for actual use
	dsnWithPassword := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s", user, password, account, database, schema, warehouse)
	if role != "" {
		dsnWithPassword += fmt.Sprintf("&role=%s", role)
	}

	// Open database connection
	log.Println("Opening Snowflake connection...")
	db, err := sql.Open("snowflake", dsnWithPassword)
	if err != nil {
		log.Fatalf("FATAL: Error opening Snowflake connection: %v", err)
	}
	defer db.Close()
	log.Println("Snowflake connection opened.")

	// Ping the database to verify connection
	log.Println("Pinging Snowflake database...")
	ctxPing, cancelPing := context.WithTimeout(context.Background(), 10*time.Second) // 10 second timeout for ping
	defer cancelPing()
	err = db.PingContext(ctxPing)
	if err != nil {
		log.Fatalf("FATAL: Error pinging Snowflake database: %v", err)
	}
	log.Println("Snowflake database ping successful.")

	// Configure connection pool
	db.SetConnMaxLifetime(time.Hour)
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(5)
	log.Println("Connection pool configured.")

	// Initialize MCP server
	log.Println("Initializing MCP server...")
	s := server.NewMCPServer(
		"snowflake-server",
		"0.1.0",
		server.WithLogging(), // Already enabled, good.
		server.WithRecovery(),
	)
	log.Println("MCP server initialized.")

	// Define execute_query tool
	execTool := mcp.NewTool(
		"execute_query",
		mcp.WithDescription("Execute a SQL query on Snowflake"),
		mcp.WithString(
			"query",
			mcp.Required(),
			mcp.Description("SQL query to execute"),
		),
	)
	log.Println("Adding 'execute_query' tool...")
	s.AddTool(execTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		log.Printf("Received tool call request: Method=%s, Params=%+v", request.Method, request.Params) // Log incoming request details (without ID)

		qRaw, ok := request.Params.Arguments["query"].(string)
		if !ok {
			log.Printf("Error: Invalid query parameter type. Expected string, got %T", request.Params.Arguments["query"])
			return mcp.NewToolResultError("invalid query parameter"), nil
		}
		q := strings.TrimSpace(qRaw)
		log.Printf("Executing query: %s", q) // Log the query itself
		up := strings.ToUpper(q)
		isWrite := strings.HasPrefix(up, "INSERT") || strings.HasPrefix(up, "UPDATE") ||
			strings.HasPrefix(up, "DELETE") || strings.HasPrefix(up, "CREATE") ||
			strings.HasPrefix(up, "DROP") || strings.HasPrefix(up, "ALTER")

		// Запрещаем выполнение операций записи для безопасности
		if isWrite {
			log.Println("Security restriction: Write operation detected. Rejecting.")
			return mcp.NewToolResultError("Security restriction: Only read operations (SELECT) are allowed."), nil
		}

		start := time.Now()
		log.Println("Querying database...")
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			log.Printf("Error executing query: %v", err)
			return mcp.NewToolResultError(fmt.Sprintf("query error: %v", err)), nil
		}
		defer rows.Close()
		log.Println("Query executed successfully.")

		cols, err := rows.Columns()
		if err != nil {
			log.Printf("Error getting columns: %v", err)
			return mcp.NewToolResultError(fmt.Sprintf("columns error: %v", err)), nil
		}
		log.Printf("Retrieved columns: %v", cols)

		var result []map[string]interface{}
		log.Println("Scanning rows...")
		rowCount := 0
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			for i := range vals {
				var v interface{}
				vals[i] = &v
			}
			if err := rows.Scan(vals...); err != nil {
				log.Printf("Error scanning row: %v", err)
				return mcp.NewToolResultError(fmt.Sprintf("scan error: %v", err)), nil
			}
			row := make(map[string]interface{})
			for i, col := range cols {
				val := *(vals[i].(*interface{}))
				row[col] = val // Consider logging row data if needed, but be mindful of size/sensitivity
			}
			result = append(result, row)
			rowCount++
		}
		log.Printf("Finished scanning %d rows.", rowCount)
		d := time.Since(start).Seconds()

		log.Println("Marshalling results to JSON...")
		payload, err := json.Marshal(result)
		if err != nil {
			log.Printf("Error marshalling JSON: %v", err)
			return mcp.NewToolResultError(fmt.Sprintf("json marshal error: %v", err)), nil
		}
		log.Println("JSON marshalled successfully.")
		response := fmt.Sprintf("Results: %s\nExecution time: %.2fs", string(payload), d)
		log.Printf("Sending tool result: %s", response) // Log before sending
		return mcp.NewToolResultText(response), nil
	})
	log.Println("'execute_query' tool added.")

	// Запускаем MCP сервер с транспортом stdio
	log.Printf("Starting MCP server with stdio transport...")
	if err := server.ServeStdio(s); err != nil {
		// Log the error from ServeStdio itself
		log.Fatalf("FATAL: Failed to serve stdio: %v", err)
	}
	log.Println("MCP Server finished.") // Should not be reached if ServeStdio runs indefinitely
} 