package blight

import (
	"encoding/binary"
	"fmt"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"

	bolt "go.etcd.io/bbolt"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type DB struct {
	db *bolt.DB
}

func New(db *bolt.DB) *DB {
	return &DB{
		db: db,
	}
}

func SetS(db *bolt.DB, bucket, key, value string) error {
	err := Set(db, []byte(bucket), []byte(key), []byte(value))
	return err
}

func (d DB) SetJSON(bucket, key string, value interface{}) error {
	j, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return Set(d.db, []byte(bucket), []byte(key), j)
}

func (d DB) Remove(bucket, key string) error {
	return DeleteS(d.db, bucket, key)
}

func (d DB) CreateBucket(bucket string) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		return err

	})

	return err
}

func (d DB) DeleteBucket(bucket string) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(bucket))
		return err

	})

	return err
}

func (d DB) SetJSONBatch(bucket, key string, value interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			logrus.Info("Recovered in SetJSONBatch", r)
		}
	}()

	j, err := json.Marshal(value)
	if err != nil {
		return err
	}

	err = d.db.Batch(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		err = bkt.Put([]byte(key), j)
		if err != nil {
			logrus.Fatal(err)
		}
		return nil
	})

	return nil
}

func (d DB) AppendJSON(bucket string, value interface{}) error {
	j, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return Append(d.db, []byte(bucket), j)
}

func Set(db *bolt.DB, bucket, key, value []byte) error {
	err := db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		err = bucket.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

func Append(db *bolt.DB, bucket, value []byte) error {
	err := db.Batch(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		// Generate ID.
		// This returns an error only if the Tx is closed or not writeable.
		// That can't happen in an Update() call so I ignore the error check.
		id, _ := bucket.NextSequence()

		err = bucket.Put(itob(id), value)
		if err != nil {
			return err
		}
		return nil
	})

	return err
}

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func Get(db *bolt.DB, bucket, key []byte) ([]byte, error) {
	var val []byte
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket %q not found", string(bucket))
		}

		val = b.Get(key)
		return nil
	})

	return val, err
}

func (d DB) GetJSON(bucket, key string, value interface{}) error {
	vs, err := Get(d.db, []byte(bucket), []byte(key))
	if err != nil {
		return err
	}
	return json.Unmarshal(vs, &value)
}

func (d DB) AllFunc(bucket string, fn func(k, v []byte)) error {

	wg := sync.WaitGroup{}
	tokens := make(chan struct{}, 30)
	for i := 0; i < 30; i++ {
		tokens <- struct{}{}
	}

	err := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %q not found", bucket)
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			<-tokens
			wg.Add(1)
			go func(k, v []byte) {
				fn(k, v)
				wg.Done()
				tokens <- struct{}{}
			}(k, v)
		}

		wg.Wait()
		return nil
	})
	return err
}

func (d DB) Close() {
	if err := d.db.Close(); err != nil {
		logrus.Fatal(err)
	}
}

func GetS(db *bolt.DB, bucket, key string) (string, error) {
	if v, err := Get(db, []byte(bucket), []byte(key)); err == nil {
		return string(v), nil
	} else {
		return "", err
	}
}

func (d DB) ResetBucket(bucket string) error {
	return ResetBucket(d.db, []byte(bucket))
}

func DeleteS(db *bolt.DB, bucket, key string) error {
	err := Delete(db, []byte(bucket), []byte(key))
	return err
}

func Delete(db *bolt.DB, bucket, key []byte) error {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return fmt.Errorf("bucket %q not found", string(bucket))
		}

		return b.Delete(key)
	})

	return err
}

func ResetBucket(db *bolt.DB, bucket []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(bucket)
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists(bucket)
		return err
	})
}

func (d DB) Set(bucket, key, value string) error {
	return SetS(d.db, bucket, key, value)
}

func (d DB) Get(bucket, key string) (string, error) {
	return GetS(d.db, bucket, key)
}
