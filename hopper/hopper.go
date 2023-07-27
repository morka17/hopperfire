package hopper

import (
	"fmt"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

const (
	defaultDBName = "default"
)

type M map[string]string

type Hopper struct {
	db *bbolt.DB
}

type Collection struct {
	name   string
	bucket *bbolt.Bucket
	db     *bbolt.DB
}

func New() (*Hopper, error) {
	dbname := fmt.Sprintf("%s.hopper", defaultDBName)
	db, err := bbolt.Open(dbname, 0666, nil)
	if err != nil {
		return nil, err
	}
	return &Hopper{
		db: db,
	}, nil
}

func (h *Hopper) CreateCollection(name string) (*Collection, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}

	return &Collection{bucket: bucket, db: h.db}, nil

}

func (h *Hopper) Insert(collName string, data M) (M, error) {

	var id uuid.UUID
	
	tx, err := h.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(collName))
	if err != nil {
		return nil, err
	}


	for k, v := range data {
		if err := bucket.Put([]byte(k), []byte(v)); err != nil {
			return nil,  err
		}
	}
	id = uuid.New()
	if err := bucket.Put([]byte("id"), []byte(id.String())); err != nil {
		return nil, err
	}

	data["_id"] = id.String()
	return data, tx.Commit()

}

// func (c *Collection) Insert(collName string, data M) (M, error) {

// 	id := uuid.New()

// 	c.db.Update(func(tx *bbolt.Tx) error {
// 		for k, v := range data {
// 			if err := c.bucket.Put([]byte(k), []byte(v)); err != nil {
// 				return err
// 			}
// 		}
// 		if err := c.bucket.Put([]byte("id"), []byte(id.String())); err != nil {
// 			return err
// 		}
// 		return nil
// 	})

// 	data["_id"] = id.String()
// 	return data, nil

// }


// get http://localhost:7777/users?eq.name=peace
func (h *Hopper) Select(coll string, query M) (M, error) {
	tx, err := h.db.Begin(false)
	if err != nil {
		return nil, err
	}

	bucket := tx.Bucket([]byte(coll))
	if bucket == nil {
		return nil, fmt.Errorf("Collection (%s) not found", coll)
	}

	
}
