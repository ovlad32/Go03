package nullable

import "testing"

func TestNewNullString(t *testing.T) {
	nullVar := NullString{}
	if nullVar.Valid() {
		t.Error("Non-initialized NullString variable is valid!")
	}

	if nullVar.Value() != "" {
		t.Error("Non-initialized NullString variable has a value!")
	}

	nonNullVar := NewNullString("test")
	if !nonNullVar.Valid() {
		t.Error("Initialized NullString invalidity is unexpected!")
	}

	if nonNullVar.Value() != "test" {
		t.Error("Initialized NullString has unexpected value!")
	}
}

func TestNullString_String(t *testing.T) {
	nullVar := NullString{}
	if nullVar.String() != "null" {
		t.Error("Result of NullBool.String() of a non-initialized variable is unexpected value!")
	}

	nonNullVar := NewNullString("test")
	if nonNullVar.String() != "test" {
		t.Error("Result of NullString(\"test\").String() is unexpected value!")
	}
}

func TestNewNullBool(t *testing.T) {
	nullVar := NullBool{}
	if nullVar.Valid() {
		t.Error("Non-initialized NullBool variable is valid!")
	}

	if nullVar.Value() {
		t.Error("Non-initialized NullBool variable has a value!")
	}

	nonNullVar := NewNullBool(true)
	if !nonNullVar.Valid() {
		t.Error("Initialized NullBool invalidity is unexpected!")
	}

	if !nonNullVar.Value() {
		t.Error("Initialized NullBool has unexpected value!")
	}
}

func TestNullBool_String(t *testing.T) {
	nullVar := NullBool{}
	if nullVar.String() != "null" {
		t.Error("Result of NullBool.String() of a non-initialized variable is unexpected value!")
	}
	type kv struct {
		value        bool
		presentation string
	}
	kvList := []kv{kv{true, "true"}, kv{false, "false"}}

	for _, sample := range kvList {
		tested := NewNullBool(sample.value)
		result := tested.String()
		if result != sample.presentation {
			t.Errorf("Result of NewNullBool(%v).String() is unexpected value (%v) !", sample.presentation, result)
		}
	}
}

func TestNewNullFloat64(t *testing.T) {
	nullVar := NullFloat64{}
	if nullVar.Valid() {
		t.Error("Non-initialized NullFloat64 variable is valid!")
	}

	if nullVar.Value() != float64(0) {
		t.Error("Non-initialized NullFloat64 variable has a value!")
	}

	nonNullVar := NewNullFloat64(float64(1))
	if !nonNullVar.Valid() {
		t.Error("Initialized NullFloat64 invalidity is unexpected!")
	}

	if nonNullVar.Value() != float64(1) {
		t.Error("Initialized NullFloat64 has unexpected value!")
	}
}

func TestNullFloat64_String(t *testing.T) {
	nullVar := NullFloat64{}
	if nullVar.String() != "null" {
		t.Error("Result of NullFloat64.String() of a non-initialized variable is unexpected value!")
	}

	type kv struct {
		value        float64
		presentation string
	}

	kvList := []kv{kv{-1, "-1"}, kv{0, "0"}, kv{1, "1"}, kv{1.1, "1.1"}, kv{-1.1, "-1.1"}}

	for _, sample := range kvList {
		tested := NewNullFloat64(sample.value)
		result := tested.String()
		if result != sample.presentation {
			t.Errorf("Result of NullFloat64(%v).String() is unexpected value (%v) !", sample.presentation, result)
		}
	}


}



func TestNewNullInt64(t *testing.T) {
	nullVar := NullInt64{}
	if nullVar.Valid() {
		t.Error("Non-initialized NullInt64 variable is valid!")
	}

	if nullVar.Value() != int64(0) {
		t.Error("Non-initialized NullInt64 variable has a value!")
	}

	nonNullVar := NewNullInt64(int64(1))
	if !nonNullVar.Valid() {
		t.Error("Initialized NullInt64 invalidity is unexpected!")
	}

	if nonNullVar.Value() != int64(1) {
		t.Error("Initialized NullInt64 has unexpected value!")
	}
}

func TestNullInt64_String(t *testing.T) {
	nullVar := NullInt64{}
	if nullVar.String() != "null" {
		t.Error("Result of NullInt64.String() of a non-initialized variable is unexpected value!")
	}

	type kv struct {
		value        int64
		presentation string
	}

	kvList := []kv{kv{-1, "-1"}, kv{0, "0"}, kv{1, "1"}}

	for _, sample := range kvList {
		tested := NewNullInt64(sample.value)
		result := tested.String()
		if result != sample.presentation {
			t.Errorf("Result of NullInt64(%v).String() is unexpected value (%v) !", sample.presentation, result)
		}
	}
}

