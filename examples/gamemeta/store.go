package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
	"rumue.io/asm/examples/gamemeta/gamepb"
)

// playerStore keeps the players in a SQLite database file shared by the nodes, standing in for a database
// (in production a database cluster). It needs no row locking: the cluster lock guarantees that only one node
// has a player open at a time.
//
// Saves are committed in batches: one writer per node commits all saves waiting at that moment in one transaction.
// A save returns once its transaction is committed, so the node getting the lock next reads the saved player.
type playerStore struct {
	db     *sql.DB
	region string
	saveC  chan saveRequest
}

type saveRequest struct {
	id   string
	data []byte
	errC chan error
}

// maxSaveBatch limits the saves committed in one transaction
const maxSaveBatch = 512

// playersDB returns the path of the players database of a demo run.
func playersDB(basePort int) string {
	return filepath.Join(storeDir(basePort), "players.db")
}

// openDB opens the database in WAL mode, so nodes read while another one writes. Writers of several nodes wait
// for each other (busy timeout) and take the write lock when their transaction begins. It is a stand-in for a
// database cluster, so it doesn't sync to disk (a crash of the machine may lose the last commits), and it reads
// through a memory map instead of a page cache per connection.
func openDB(path string) (*sql.DB, error) {
	dsn := "file:" + filepath.ToSlash(path) +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)&_pragma=busy_timeout(30000)" +
		"&_pragma=mmap_size(268435456)&_pragma=cache_size(-16384)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// loads queue for a few connections instead of opening one per waiting session
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	return db, nil
}

// createPlayersDB creates the empty database before the nodes start.
func createPlayersDB(path string) error {
	db, err := openDB(path)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS players (id TEXT PRIMARY KEY, data BLOB NOT NULL)`)
	return err
}

func openPlayerStore(path, region string) (*playerStore, error) {
	db, err := openDB(path)
	if err != nil {
		return nil, err
	}
	s := &playerStore{db: db, region: region, saveC: make(chan saveRequest, 4*maxSaveBatch)}
	go s.writer()
	return s, nil
}

func (s *playerStore) load(ctx context.Context, c *playerContainer) error {
	p := newPlayer(c.Id(), s.region)

	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data FROM players WHERE id = ?`, c.Id()).Scan(&data)
	switch {
	case err == nil:
		p = &gamepb.Player{}
		if err := proto.Unmarshal(data, p); err != nil {
			return err
		}
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("loading player %s: %w", c.Id(), err)
	}

	c.Builder().Set(p)
	return nil
}

func (s *playerStore) save(_ context.Context, c *playerContainer) error {
	p := c.Builder().Build()
	if p == nil {
		return nil
	}

	data, err := proto.Marshal(p)
	if err != nil {
		return err
	}
	errC := make(chan error, 1)
	s.saveC <- saveRequest{id: c.Id(), data: data, errC: errC}
	if err := <-errC; err != nil {
		return fmt.Errorf("saving player %s: %w", c.Id(), err)
	}
	return nil
}

// writer commits the waiting saves together.
func (s *playerStore) writer() {
	for req := range s.saveC {
		batch := []saveRequest{req}
	more:
		for len(batch) < maxSaveBatch {
			select {
			case r := <-s.saveC:
				batch = append(batch, r)
			default:
				break more
			}
		}

		err := s.commit(batch)
		for _, r := range batch {
			r.errC <- err
		}
	}
}

func (s *playerStore) commit(batch []saveRequest) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO players (id, data) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, r := range batch {
		if _, err := stmt.Exec(r.id, r.data); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
