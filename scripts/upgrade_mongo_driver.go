package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	rootDir := "d:/Golang/modules/cosmo"
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		updateFileImports(path)
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking directory: %v\n", err)
	}
}

func updateFileImports(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", filePath, err)
		return
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	modified := false

	for scanner.Scan() {
		line := scanner.Text()
		newLine := updateImportLine(line)
		if newLine != line {
			modified = true
			lines = append(lines, newLine)
		} else {
			lines = append(lines, line)
		}
	}

	if modified {
		writeFile(filePath, lines)
		fmt.Printf("Updated: %s\n", filePath)
	}
}

func updateImportLine(line string) string {
	oldImports := []string{
		`"go.mongodb.org/mongo-driver/v2/mongo"`,
		`"go.mongodb.org/mongo-driver/v2/mongo/options"`,
		`"go.mongodb.org/mongo-driver/v2/mongo/readpref"`,
		`"go.mongodb.org/mongo-driver/v2/mongo/readconcern"`,
		`"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"`,
		`"go.mongodb.org/mongo-driver/v2/bson"`,
		`"go.mongodb.org/mongo-driver/v2/bson/primitive"`,
	}

	newImports := []string{
		`"go.mongodb.org/mongo-driver/v2/mongo"`,
		`"go.mongodb.org/mongo-driver/v2/mongo/options"`,
		`"go.mongodb.org/mongo-driver/v2/mongo/readpref"`,
		`"go.mongodb.org/mongo-driver/v2/mongo/readconcern"`,
		`"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"`,
		`"go.mongodb.org/mongo-driver/v2/bson"`,
		`"go.mongodb.org/mongo-driver/v2/bson/primitive"`,
	}

	for i, oldImport := range oldImports {
		if strings.Contains(line, oldImport) {
			return strings.Replace(line, oldImport, newImports[i], 1)
		}
	}

	return line
}

func writeFile(filePath string, lines []string) {
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("Error creating file %s: %v\n", filePath, err)
		return
	}
	defer file.Close()

	for _, line := range lines {
		file.WriteString(line + "\n")
	}
}
