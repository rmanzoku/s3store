package s3store_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/rmanzoku/s3store"
)

var (
	s      *s3store.S3Store
	bucket = os.Getenv("BUCKET")
)

func TestMain(m *testing.M) {
	var err error
	s, err = s3store.NewS3Store(bucket)
	if err != nil {
		panic(err)
	}
	code := m.Run()
	os.Exit(code)
}

func TestSet(t *testing.T) {
	f, err := os.Open("fixture/sample.png")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	body, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	type args struct {
		key  string
		body []byte
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Set ",
			args: args{
				key:  "testing/sample.png",
				body: body,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.Put(tt.args.key, tt.args.body); (err != nil) != tt.wantErr {
				t.Errorf("S3Store.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGet(t *testing.T) {

	type args struct {
		key string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "testing/sssssample.png",
			args: args{
				key: "testing/sssssample.png",
			},
			wantErr: true,
		},
		{
			name: "testing/sample.png",
			args: args{
				key: "testing/sample.png",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.Get(tt.args.key)
			t.Log(err)
			if (err != nil) != tt.wantErr {
				t.Errorf("S3Store.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			_ = ioutil.WriteFile("results/sample.png", got, 0777)
		})
	}
}
