package data

import "time"

//Post ...
type Post struct {
	UserID   int       `index:"UID"`
	Created  time.Time `index:"PostCreated"`
	Title    string
	Text     string
	Tags     []string `index:"Tag"`
	Comments []Comment
}

//Comment ...
type Comment struct {
	UserID   int
	Created  time.Time `index:"CommentCreated"`
	Text     string
	Comments []Comment
}

//User ...
type User struct {
	username string `index:"Username"`
	password string
}
