package main

import "net/http"

func index(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`<html>
        </html>`))
}

func main() {
	http.HandleFunc("/", index)
	http.ListenAndServe(":8080", nil)
}
