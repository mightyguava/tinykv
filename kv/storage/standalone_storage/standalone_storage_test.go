package standalone_storage

import (
	"os"
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	dir, err := os.MkdirTemp("", "tinykv")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	store := NewStandAloneStorage(&config.Config{DBPath: dir})
	err = store.Write(nil, []storage.Modify{
		{Data: storage.Put{Cf: "superheroes", Key: []byte("batman"), Value: []byte("robin")}},
		{Data: storage.Put{Cf: "superheroes", Key: []byte("superman"), Value: []byte("lois lane")}},
		{Data: storage.Put{Cf: "superheroes", Key: []byte("flash"), Value: []byte("dunno")}},
		{Data: storage.Put{Cf: "animals", Key: []byte("monkey"), Value: []byte("banana")}},
	})
	require.NoError(t, err)

	r, err := store.Reader(nil)
	require.NoError(t, err)
	iter := r.IterCF("superheroes")
	iter.Seek(nil)
	require.True(t, iter.Valid())
	item := iter.Item()
	require.Equal(t, "batman", string(item.Key()))
	v, err := item.Value()
	require.NoError(t, err)
	require.Equal(t, "robin", string(v))
	iter.Next()
	require.True(t, iter.Valid())
	item = iter.Item()
	require.Equal(t, "flash", string(item.Key()))
	v, err = item.Value()
	require.NoError(t, err)
	require.Equal(t, "dunno", string(v))
}
