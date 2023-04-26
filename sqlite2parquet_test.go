package sqlite2parquet

import (
	"bytes"
	"database/sql"
	"log"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/segmentio/parquet-go"
)

type record struct {
	ID         string  `parquet:"id"`
	Title      *string `parquet:"title"`
	Length     int     `parquet:"length"`
	Decimal    float64 `parquet:"decimal"`
	Active     bool    `parquet:"active"`
	Binary     []byte  `parquet:"binary"`
	Untypedcol string  `parquet:"untypedcol"`
}

func TestSqlite2parquet(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE warmongering (
id text NOT NULL PRIMARY KEY,
title TEXT,
length INTEGER,
decimal REAL,
active BOOLEAN,
binary BLOB,
untypedcol
)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`INSERT INTO warmongering (id, title, length, decimal, active, binary, untypedcol) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"surrendering-tektite", "twentieth-willpower", 999, 958.874, false, []byte("Seattle-lightheartedness"), 1234)
	if err != nil {
		t.Fatal(err)
	}

	records := []record{
		{
			ID:         "forthcoming-attaching",
			Length:     113,
			Decimal:    948.599,
			Active:     true,
			Binary:     []byte("sluggishly-chignon"),
			Untypedcol: "mundanes-doctrinaire",
		},
		{
			ID:         "crumpet-Drambuie",
			Title:      stringPtr("PVC-malingerer"),
			Length:     -183,
			Decimal:    0.000,
			Active:     false,
			Binary:     []byte("unfashionably-antipathetic"),
			Untypedcol: string([]byte{0xff, 0xff, 0x01}),
		},
		{
			ID:         "mismatched-leniency",
			Title:      stringPtr("foresightedness-substitution"),
			Length:     0,
			Decimal:    -407.196,
			Active:     true,
			Binary:     []byte{0xff, 0xff, 0x01, 0x02, 0x03, 0x04},
			Untypedcol: "soreness-Stuart",
		},
	}

	for _, r := range records {
		_, err = db.Exec(`INSERT INTO warmongering (id, title, length, decimal, active, binary, untypedcol) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			r.ID, r.Title, r.Length, r.Decimal, r.Active, r.Binary, r.Untypedcol)
		if err != nil {
			t.Fatal(err)
		}
	}

	var buf bytes.Buffer

	err = ExportTable(db, "warmongering", &buf)
	if err != nil {
		t.Fatal(err)
	}

	file := bytes.NewReader(buf.Bytes())

	rows, err := parquet.Read[record](file, file.Size())
	if err != nil {
		log.Fatal(err)
	}

	expect := []record{
		{
			ID:         "surrendering-tektite",
			Title:      stringPtr("twentieth-willpower"),
			Length:     999,
			Decimal:    958.874,
			Active:     false,
			Binary:     []byte("Seattle-lightheartedness"),
			Untypedcol: "1234",
		},
	}

	expect = append(expect, records...)

	if !cmp.Equal(rows, expect) {
		t.Fatal(cmp.Diff(rows, expect))
	}
}

func stringPtr(s string) *string {
	return &s
}
