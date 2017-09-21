package dataflow

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestColumnBlockType_Append(t *testing.T) {
	var expected []byte
	var index = 0
	var columns []int64
	tested := &ColumnBlockType{}

	columns = tested.Append(int64(35), uint64(1))

	expected = make([]byte, 8*4)
	index = -8
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 1 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 1 {
		t.Errorf("Scenario 1 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 1, len(columns))
	}

	columns = tested.Append(int64(35), uint64(128))

	expected = make([]byte, 8*6)
	index = -8
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 2 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 1 {
		t.Errorf("Scenario 2 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 1, len(columns))
	}

	columns = tested.Append(int64(35), uint64(3))

	expected = make([]byte, 8*6)
	index = -8
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0xA)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 3 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 1 {
		t.Errorf("Scenario 3 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 1, len(columns))
	}

	columns = tested.Append(int64(35), uint64(384))

	expected = make([]byte, 8*8)
	index = -8
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0xA)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 4 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 1 {
		t.Errorf("Scenario 4 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 1, len(columns))
	}

	columns = tested.Append(int64(34), uint64(3))

	expected = make([]byte, 8*12)
	index = -8
	// 35 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0xA)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	// 34 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 34)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 5 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 2 {
		t.Errorf("Scenario 5 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 2, len(columns))
	}

	columns = tested.Append(int64(35), uint64(6))

	expected = make([]byte, 8*12)
	index = -8
	// 35 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0x4A) //v
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	// 34 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 34)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 6 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 2 {
		t.Errorf("Scenario 6 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 2, len(columns))
	}

	columns = tested.Append(int64(36), uint64(2))
	expected = make([]byte, 8*16)
	index = -8
	// 35 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0x4A)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	// 34 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 34)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)

	// 36 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 36)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 4)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 7 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 3 {
		t.Errorf("Scenario 7 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 3, len(columns))
	}

	columns = tested.Append(int64(35), uint64(129))

	expected = make([]byte, 8*16)
	index = -8
	// 35 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0x4A)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3) //v
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	// 34 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 34)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)

	// 36 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 36)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 4)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 8 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 3 {
		t.Errorf("Scenario 8 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 3, len(columns))
	}

	columns = tested.Append(int64(34), uint64(323))

	expected = make([]byte, 8*18)
	index = -8
	// 35 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0x4A)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	// 34 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 34)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 5) //v
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8) //v

	// 36 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 36)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 4)

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 9 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 3 {
		t.Errorf("Scenario 9 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 3, len(columns))
	}
	columns = tested.Append(int64(36), uint64(222))
	expected = make([]byte, 8*20)
	index = -8
	// 35 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0x4A)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	// 34 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 34)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 5)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)
	// 36 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 36)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 4)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3) //v
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], (1 << 30)) //v

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 10 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}

	if len(columns) != 3 {
		t.Errorf("Scenario 10 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 3, len(columns))
	}

	columns = tested.Append(int64(34), uint64(623))

	expected = make([]byte, 8*22)
	index = -8
	// 35 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0x4A)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	// 34 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 34)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 5)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 9) //v
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], (1 << 47)) //v
	// 36 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 36)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 4)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], (1 << 30))

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 11 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 3 {
		t.Errorf("Scenario 11 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 3, len(columns))
	}
	columns = tested.Append(int64(35), uint64(600))

	expected = make([]byte, 8*24)
	index = -8
	// 35 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 35)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 4)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0x4A)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 6)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 1)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 9) //v
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], (1 << 24)) //v
	// 34 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 34)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 5)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 8)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 9)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], (1 << 47))
	// 36 section
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 36)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 2)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 0)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 4)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], 3)
	index += 8
	binary.LittleEndian.PutUint64(expected[index:], (1 << 30))

	if !reflect.DeepEqual(tested.Data, expected) {
		t.Errorf("Scenario 12 mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", expected, tested.Data)
	}
	if len(columns) != 3 {
		t.Errorf("Scenario 12 column mismatch!\n"+
			"Expected: %v\n"+
			"Got     : %v\n", 3, len(columns))
	}

}
