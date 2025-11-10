package menu

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/buger/goterm"
	"github.com/pkg/term"
)

// Terminal control codes
const (
	ShowCursor = "\033[?25h"
	HideCursor = "\033[?25l"
	// CursorUpFormat Requires formatting with number of lines
	CursorUpFormat = "\033[%dA"
	ClearLine      = "\r\033[K"
	KeyUp          = byte(65)
	KeyDown        = byte(66)
	KeyLeft        = byte(68)
	KeyRight       = byte(67)
	KeyEscape      = byte(27)
	KeyEnter       = byte(13)
	KeyTab         = byte(9) // Tab 鍵
)

// NavigationKeys
var NavigationKeys = map[byte]bool{
	KeyUp:    true,
	KeyDown:  true,
	KeyLeft:  true,
	KeyRight: true,
}

type Menu struct {
	Prompt string

	// Options (Horizontal Menu)
	Options      []string
	OptionCursor int

	// Table (Vertical Menu)
	SortedId    []uint64
	TableLines  []string
	TableCursor int

	// State
	SelectTable bool

	// Display partial rows
	DisplayRows int
	StartingRow int
}

func NewMenu(prompt string, tableContent string, sortedId []uint64, options []string, selectTable bool) *Menu {
	if len(sortedId) > 0 {
		tablelines := strings.Split(tableContent, "\n") // Table
		if len(tablelines) > 0 && tablelines[len(tablelines)-1] == "" {
			tablelines = tablelines[:len(tablelines)-1]
		}
		return &Menu{
			Prompt:      prompt,
			Options:     options, // Options
			SortedId:    sortedId,
			TableLines:  tablelines,
			SelectTable: selectTable,
			DisplayRows: min(len(tablelines)-4, 10),
		}
	}
	return &Menu{
		Prompt:      prompt,
		Options:     options, // Options
		SelectTable: false,
	}
}

func (m *Menu) renderUI(redraw bool) {
	// Render the whole UI; if redraw=true, move cursor to top

	if redraw {
		// Calculate total line count
		tableLines := 0
		if m.TableLines != nil {
			tableLines = m.DisplayRows + 4 // Row + header(3) + footer(1)
		}
		totalLines := tableLines + 2

		// Move the cursor to the top
		fmt.Printf(CursorUpFormat, totalLines)
	}

	// Print Prompt
	fmt.Printf("%s\n", goterm.Color(goterm.Bold(m.Prompt)+":", goterm.CYAN))

	// Print Options
	optionLine := ""
	for i, opt := range m.Options {
		if i == m.OptionCursor {
			optionLine += goterm.Color(fmt.Sprintf("   >%s   ", opt), goterm.YELLOW)
		} else {
			optionLine += fmt.Sprintf("    %s    ", opt)
		}
	}

	fmt.Print(ClearLine) // Clear current line
	fmt.Print(optionLine + "\n")

	if m.TableLines != nil {
		// Render table
		headerLines := 3

		// Calculate StartingRow
		if m.TableCursor < m.StartingRow {
			m.StartingRow = m.TableCursor
		}
		if m.TableCursor >= m.StartingRow+m.DisplayRows {
			m.StartingRow = m.TableCursor - m.DisplayRows + 1
		}

		dataLines := m.TableLines[headerLines+m.StartingRow : headerLines+m.StartingRow+m.DisplayRows]
		numDataRows := len(dataLines)

		// Print the header
		fmt.Print(ClearLine + m.TableLines[0] + "\n")
		fmt.Print(ClearLine + m.TableLines[1] + "\n")
		fmt.Print(ClearLine + m.TableLines[2] + "\n")

		// Print rows
		termHeight := goterm.Height()
		for i := 0; i < min(termHeight, numDataRows); i++ {
			line := dataLines[i]
			fmt.Print(ClearLine) // Clear current line
			if i == m.TableCursor-m.StartingRow && m.SelectTable {
				fmt.Print(goterm.Color(line, goterm.YELLOW) + "\n") // Highlight current line
			} else {
				fmt.Print(line + "\n")
			}
		}

		// Print the footer
		fmt.Print(ClearLine + m.TableLines[len(m.TableLines)-1] + "\n")
	}

	goterm.Flush()
}

func (m *Menu) Display() (string, uint64, error) {
	defer func() {
		// restore cursor
		fmt.Print(ShowCursor)
		goterm.Clear()
		goterm.MoveCursor(1, 1)
		goterm.Flush()
	}()

	// first render
	m.renderUI(false)
	fmt.Print(HideCursor)

	// deal with SIGTERM
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)

	for {
		keyCode := getInput()
		numDataRows := len(m.SortedId)

		switch keyCode {
		case KeyEscape: // Exit
			return "", 0, fmt.Errorf("exit")

		case KeyTab: // switch context
			m.OptionCursor = (m.OptionCursor + len(m.Options) - 1) % len(m.Options)

		case KeyLeft:
			m.OptionCursor = (m.OptionCursor + len(m.Options) - 1) % len(m.Options)

		case KeyRight:
			m.OptionCursor = (m.OptionCursor + 1) % len(m.Options)

		case KeyUp:
			if numDataRows > 0 {
				m.TableCursor = (m.TableCursor + numDataRows - 1) % numDataRows
			}
			
		case KeyDown:
			if numDataRows > 0 {
				m.TableCursor = (m.TableCursor + 1) % numDataRows
			}

		case KeyEnter: // selection
			if m.SelectTable {
				// If press enter on table, return selected value
				selectedOption := m.Options[m.OptionCursor]
				selectedRowID := m.SortedId[m.TableCursor]
				goterm.Clear()
				goterm.MoveCursor(1, 1)
				goterm.Flush()
				fmt.Print(ShowCursor)
				return selectedOption, selectedRowID, nil
			} else if !m.SelectTable {
				selectedOption := m.Options[m.OptionCursor]
				goterm.Clear()
				goterm.MoveCursor(1, 1)
				goterm.Flush()
				fmt.Print(ShowCursor)
				return selectedOption, 0, nil
			}
		}
		m.renderUI(true)
	}
}

func getInput() byte {
	t, _ := term.Open("/dev/tty")
	defer t.Close()

	err := term.RawMode(t)
	if err != nil {
		log.Fatal(err)
	}

	var read int
	readBytes := make([]byte, 3)
	read, err = t.Read(readBytes)
	if err != nil {
		return 0
	}

	defer t.Restore()

	if read == 3 {
		if _, ok := NavigationKeys[readBytes[2]]; ok {
			return readBytes[2]
		}
	} else {
		return readBytes[0]
	}

	return 0
}
