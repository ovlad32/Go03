package dataflow

import "testing"

type columnType struct {
	isNumeric bool;
}
func (c columnType) IsNumericDataType() bool{
	return c.isNumeric
}


func TestNewDataCategory(t *testing.T) {
	numeric := &columnType{true};
	nonNumeric := &columnType{false};

	type testCaseType struct {
		Scenario int
		Data []byte
		Column *columnType
		Expected *DataCategorySimpleType
	}
	var testCasSet = []testCaseType{
		testCaseType{
			Scenario:1,
			Data: []byte("12345"),
			Column: numeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: true,
				IsNegative: false,
				FloatValue: float64(12345),
			},
		},
		testCaseType{
			Scenario:2,
			Data: []byte("abcde"),
			Column: nonNumeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: false,
			},
		},
		testCaseType{
			Scenario:3,
			Data: []byte("1E3"),
			Column: nonNumeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: false,
			},
		},
		testCaseType{
			Scenario:4,
			Data: []byte("1.1E3"),
			Column: numeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: true,
				FloatValue:float64(1100),
			},
		},
		testCaseType{
			Scenario:5,
			Data: []byte("1.1E-3"),
			Column: nonNumeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: true,
				FloatValue:float64(1100),
			},
		},
		testCaseType{
			Scenario:6,
			Data: []byte(".1E3"),
			Column: nonNumeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: true,
				FloatValue:float64(100),
			},
		},
		testCaseType{
			Scenario:7,
			Data: []byte("0E3"),
			Column: nonNumeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: false,
			},
		},
		testCaseType{
			Scenario:8,
			Data: []byte("-1.1E3"),
			Column: nonNumeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: true,
				IsNegative:true,
				FloatingPointScale:0,
				FloatValue:float64(-1100),
			},
		},
		testCaseType{
			Scenario:8,
			Data: []byte("-1.1E-3"),
			Column: nonNumeric,
			Expected: &DataCategorySimpleType{
				IsNumeric: true,
				IsNegative:true,
				FloatingPointScale:4,
				FloatValue:float64(-0.0011),
			},
		},
	}

	for _, testCase := range testCasSet {
		result := NewDataCategory(
			testCase.Data,
			testCase.Column,
		);

		if result.IsNumeric != testCase.Expected.IsNumeric {
			t.Errorf("Case %v Data %v : IsNumeric:%v but expected %v",
				testCase.Scenario,
				string(testCase.Data),
				result.IsNumeric,
				testCase.Expected.IsNumeric,
			)
		} else if testCase.Expected.IsNumeric {

			if result.IsNegative != testCase.Expected.IsNegative {
				t.Errorf("Case %v Data %v : IsNegative:%v but expected %v",
					testCase.Scenario,
					string(testCase.Data),
					result.IsNegative,
					testCase.Expected.IsNegative,
				)
			}

			if result.FloatingPointScale!= testCase.Expected.FloatingPointScale {
				t.Errorf("Case %v Data %v : FloatingPointScale:%v but expected %v",
					testCase.Scenario,
					string(testCase.Data),
					result.FloatingPointScale,
					testCase.Expected.FloatingPointScale,
				)
			}

			if result.FloatValue!= testCase.Expected.FloatValue {
				t.Errorf("Case %v Data %v : FloatValue:%v but expected %v",
					testCase.Scenario,
					string(testCase.Data),
					result.FloatValue,
					testCase.Expected.FloatValue,
				)
			}

		}

	}

}