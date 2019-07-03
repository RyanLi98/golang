package main

import (
	"bytes"
	"io/ioutil"
	"log"

	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPUTandGET(t *testing.T) {
	// Save
	body := bytes.NewBufferString(`{"Foo":1,"Bar":"two"}`)
	r, err := http.NewRequest("PUT", "http://localhost:9090/quote/12", body)
	r.Header.Add("Content-Type", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	HandleEvent(w, r)
	if w.Result().StatusCode != 200 {
		t.Fatal(w.Result().Status)
	}

	// Recall
	r, err = http.NewRequest("GET", "http://localhost:9090/quote/12", body)
	r.Header.Add("Accept", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	HandleEvent(w, r)
	if w.Result().StatusCode != 200 {
		t.Fatal(w.Result().Status)
	}
	resp, err := ioutil.ReadAll(w.Result().Body)
	if string(resp) != `{"Bar":"two","Foo":1}` {
		t.Errorf(`expected: {"Bar":"two","Foo":1} got: %s`, string(resp))
	}
	// event.DumpBucket("Q12")

}
func TestPUT2andGET(t *testing.T) {
	// Save
	body := bytes.NewBufferString(`{"Foo":1,"Bar":"two"}`)
	r, err := http.NewRequest("PUT", "http://localhost:9090/quote/13", body)
	r.Header.Add("Content-Type", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	HandleEvent(w, r)
	if w.Result().StatusCode != 200 {
		t.Fatal(w.Result().Status)
	}
	body = bytes.NewBufferString(`{"Foo":2,"Animal":{"Dog": "bark", "Cat": "meow"}}`)
	r, err = http.NewRequest("PUT", "http://localhost:9090/quote/13", body)
	r.Header.Add("Content-Type", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	HandleEvent(w, r)
	if w.Result().StatusCode != 200 {
		t.Fatal(w.Result().Status)
	}

	// Recall
	r, err = http.NewRequest("GET", "http://localhost:9090/quote/13", body)
	r.Header.Add("Accept", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	HandleEvent(w, r)
	if w.Result().StatusCode != 200 {
		t.Fatal(w.Result().Status)
	}
	resp, err := ioutil.ReadAll(w.Result().Body)
	log.Print(string(resp))
}

func BenchmarkSave(b *testing.B) {
	for n := 0; n < b.N; n++ {
		// Save
		body := bytes.NewBufferString(`{"Foo":1,"Bar":"two"}`)
		r, err := http.NewRequest("PUT", "http://localhost:9090/quote/12", body)
		r.Header.Add("Content-Type", "application/json")
		if err != nil {
			b.Fatal(err)
		}
		w := httptest.NewRecorder()
		HandleEvent(w, r)
		if w.Result().StatusCode != 200 {
			b.Fatal(w.Result().Status)
		}
	}
}

func TestBadURL(t *testing.T) {
	// Save
	body := bytes.NewBufferString(`{"Foo":1,"Bar":"two"}`)
	r, err := http.NewRequest("PUT", "http://localhost:9090/12", body)
	r.Header.Add("Content-Type", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	HandleEvent(w, r)
	b, err := ioutil.ReadAll(w.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(b, []byte("bad request")) {
		t.Errorf("nowarning in body: %s", string(b))
	}
	if w.Result().StatusCode == 200 {
		t.Errorf("%+v", w.Result())
	}
}

func BenchmarkRecall(b *testing.B) {
	body := new(bytes.Buffer)
	for n := 0; n < b.N; n++ {
		// Recall
		r, err := http.NewRequest("GET", "http://localhost:9090/quote/12", body)
		r.Header.Add("Content-Type", "application/json")
		if err != nil {
			b.Fatal(err)
		}
		w := httptest.NewRecorder()
		HandleEvent(w, r)
		if w.Result().StatusCode != 200 {
			b.Fatal(w.Result().Status)
		}
		resp, err := ioutil.ReadAll(w.Result().Body)
		if string(resp) != `{"Bar":"two","Foo":1}` {
			b.Errorf(`expected: {"Bar":"two","Foo":1} got: %s`, string(resp))
		}
	}
}
