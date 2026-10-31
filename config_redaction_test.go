package particledb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestConfigFormattingRedactsCredentials(t *testing.T) {
	cfg, err := ParseConfig("particledb://app:private-credential@localhost")
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(format, cfg), "private-credential") {
			t.Fatal("credential exposed in config formatting")
		}
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "private-credential") {
		t.Fatal("credential exposed in config JSON")
	}
}

func TestConfigParseErrorsRedactCredentials(t *testing.T) {
	for i, dsn := range []string{
		"particledb://app:private-credential@localhost:invalid",
		"pdb://app:private-credential@localhost/%zz",
		"host=localhost password private-credential",
		"host=localhost =private-credential",
		"host=localhost password=private-credential port=private-credential",
		"host=localhost private-credential='",
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			_, err := ParseConfig(dsn)
			if err == nil {
				t.Fatal("expected malformed connection string to fail")
			}
			for err != nil {
				for _, format := range []string{"%v", "%+v", "%#v"} {
					if strings.Contains(fmt.Sprintf(format, err), "private-credential") {
						t.Fatal("credential exposed in connection string parse error")
					}
				}
				err = errors.Unwrap(err)
			}
		})
	}
}
