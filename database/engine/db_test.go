package engine

import (
	"Dawndis/interface/database"
	"testing"
	"time"
)

var (
	k1        = "key1"
	v1        = &database.DataEntity{Data: "value1"}
	k2        = "key2"
	v2        = &database.DataEntity{Data: "value2"}
	v2Updated = &database.DataEntity{Data: "new value2"}
	k3        = "key3"
	v3        = &database.DataEntity{Data: "value3"}
)

func TestDB(t *testing.T) {
	db := makeDB()

	/* Test Put And Get */
	t.Log("Test Put And Get")
	db.PutEntity(k1, v1)
	e, ok := db.GetEntity(k1)
	if !ok || e != v1 {
		t.Error("PutEntity and GetEntity error")
	}
	t.Log("k1=", k1, "v1=", e)

	db.PutIfAbsent(k1, &database.DataEntity{Data: "new value1"})
	e, ok = db.GetEntity(k1)
	if !ok || e != v1 {
		t.Error("PutIfAbsent and GetEntity error")
	}
	t.Log("k1=", k1, "v1=", e)

	db.PutEntity(k2, v2)
	db.PutIfExists(k2, v2Updated)
	e, ok = db.GetEntity(k2)
	if !ok || e != v2Updated {
		t.Log("k2=", k2, "v2=", e)
		t.Error("PutIfExists and GetEntity error")
	}
	t.Log("k2=", k2, "v2=", e)

	db.PutIfAbsent(k3, v3)
	e, ok = db.GetEntity(k3)
	if !ok || e != v3 {
		t.Error("PutIfAbsent and GetEntity error")
	}
	t.Log("k3=", k3, "v3=", e)

	/* Test TTL Func */
	t.Log("Test TTL Func")
	db.ExpireAfter(k3, time.Second*2)
	time.Sleep(time.Second * 1)
	e, ok = db.GetEntity(k3)
	if !ok || e != v3 {
		t.Error("ExpireAfter error")
	}
	t.Log("k3=", k3, "v3=", e)
	time.Sleep(time.Second * 1)
	e, ok = db.GetEntity(k3)
	if ok {
		t.Error("ExpireAfter error")
	}
	t.Log("k3=", k3, "expired")

	db.ExpireAfter(k1, time.Second*2)
	time.Sleep(time.Second * 1)
	e, ok = db.GetEntity(k1)
	if !ok || e != v1 {
		t.Error("PutIfAbsent and GetEntity error")
	}
	t.Log("k1=", k1, "v1=", e)
	db.Persist(k1)
	time.Sleep(time.Second * 1)
	e, ok = db.GetEntity(k1)
	if !ok || e != v1 {
		t.Error("PutIfAbsent and GetEntity error")
	}
	t.Log("k1=", k1, "v1=", e)
}
