# Blight
Blight is the boilerplate around bbolt that I found myself copying out
of one project into another, then stopped to put it here.

```go
db := blight.New(bdb)
db.Set("bucket", "key", "value")
v, err := db.Get("bucket", "key")

v == "value"
```

It isn't much, but it makes my life easier.