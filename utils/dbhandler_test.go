package utils

import (
	"fmt"
	"testing"
)

func TestDBHandler_ShowSlaveStatus(t *testing.T) {
	var (
		dbHandler *DBHandler
		err       error
	)
	if dbHandler, err = NewDBHandler("192.168.31.101", 3340, "root", "root"); err != nil {
		panic(err)
	}
	if slaves, err := dbHandler.ShowSlaveStatus(); err != nil {
		panic(err)
	} else {
		for _, s := range slaves {
			for k, v := range s {
				fmt.Printf("%s=%s\n", k, v)
			}
		}
	}
}

func TestDBHandler_CreateUser(t *testing.T) {
	var (
		dbHandler *DBHandler
		err       error
	)
	if dbHandler, err = NewDBHandler("192.168.31.101", 3340, "root", "root"); err != nil {
		panic(err)
	}
	user := "test"
	host := "%"
	password := "test"
	if err := dbHandler.CreateUser(user, host, password, ""); err != nil {
		t.Error(err)
	} else {
		t.Logf("创建用户:%s 成功", user)
	}
	if err := dbHandler.DropUser(user, host); err != nil {
		t.Error(err)
	} else {
		t.Logf("删除用户:%s 成功", user)
	}
}

func TestDBHandler_CopyUser(t *testing.T) {
	var (
		dbHandler *DBHandler
		err       error
	)
	if dbHandler, err = NewDBHandler("192.168.31.101", 3340, "root", "root"); err != nil {
		panic(err)
	}
	if err := dbHandler.CopyUser("root", "%", "root1", "%"); err != nil {
		t.Error(err)
	} else {
		t.Logf("复制用户:%s成功", "root")
	}
}

func TestDBHandler_KillSessionByUser(t *testing.T) {
	var (
		dbHandler *DBHandler
		err       error
	)
	if dbHandler, err = NewDBHandler("192.168.31.101", 3340, "root", "root"); err != nil {
		panic(err)
	}
	if err := dbHandler.KillSessionByUser("root1"); err != nil {
		t.Error(err)
	} else {
		t.Log("删除成功")
	}
}

func TestDBHandler_KillSessionByClientHost(t *testing.T) {
	var (
		dbHandler *DBHandler
		err       error
	)
	if dbHandler, err = NewDBHandler("192.168.31.101", 3340, "root", "root"); err != nil {
		panic(err)
	}
	if err := dbHandler.KillSessionByClientHost("t-test-db01"); err != nil {
		t.Error(err)
	} else {
		t.Log("删除成功")
	}
}
