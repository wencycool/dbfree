package utils

import (
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type DBHandler struct {
	conn *sql.DB
}

func NewDBHandler(host string, port int, user, password string) (*DBHandler, error) {
	self := new(DBHandler)
	db, err := newDBConn(host, port, user, password)
	if err != nil {
		return nil, err
	}
	self.conn = db
	return self, nil
}

//查看数据库版本
func (d *DBHandler) GetVersion() ([3]int, error) {
	var versionText string
	row := d.conn.QueryRow("select version()")
	if err := row.Scan(&versionText); err != nil {
		return [3]int{0, 0, 0}, err
	}
	patt := `^(?P<v>\d+\.\d+\.\d+)`
	subMatch := regexp.MustCompile(patt).FindStringSubmatch(versionText)
	if len(subMatch) == 0 {
		return [3]int{0, 0, 0}, errors.New(fmt.Sprintf(" 无法利用正则表达式:%s从版本:%s中解析出版本号", patt, versionText))
	}
	versionList := strings.Split(subMatch[1], ".")
	fisrtV, _ := strconv.Atoi(versionList[0])
	secondV, _ := strconv.Atoi(versionList[1])
	thirdV, _ := strconv.Atoi(versionList[2])
	return [3]int{fisrtV, secondV, thirdV}, nil
}

//新增一个数据库
func (d *DBHandler) CreateDB(dbname, charset, collate string) error {
	createDBSQL := fmt.Sprintf("CREATE DATABASE %s CHARACTER SET = %s COLLATE = %s", dbname, charset, collate)
	_, err := d.conn.Exec(createDBSQL)
	return err
}

//删除一个数据库
func (d *DBHandler) DropDB(dbname string) error {
	dropDBSQL := fmt.Sprintf("DROP DATABASE %s", dbname)
	_, err := d.conn.Exec(dropDBSQL)
	return err
}

//根据用户名和主机拼接成mysql的用户形式
func userName(user, host string) string {
	if host == "" {
		return fmt.Sprintf("'%s'", user)
	} else {
		return fmt.Sprintf("'%s'@'%s'", user, host)
	}

}

//创建一个用户
func (d *DBHandler) CreateUser(user, host, password, plugin string) error {
	username := userName(user, host)
	var createUserSQL string
	switch plugin {
	case "":
		createUserSQL = fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s'", username, plugin)
	case "mysql_native_password":
		createUserSQL = fmt.Sprintf("CREATE USER %s IDENTIFIED WITH %s BY '%s'", username, plugin, password)
	}
	_, err := d.conn.Exec(createUserSQL)
	return err
}

//删除一个用户
func (d *DBHandler) DropUser(user, host string) error {
	username := userName(user, host)
	var dropUserSQL string
	dropUserSQL = fmt.Sprintf("DROP USER %s", username)
	_, err := d.conn.Exec(dropUserSQL)
	return err
}

//修改一个用户的密码
func (d *DBHandler) AlterUser(user, host, password string) error {
	username := userName(user, host)
	var alterUserSQL string
	var plugin string
	//查找当前用户的plugin选项
	row := d.conn.QueryRow(fmt.Sprintf("SELECT PLUGIN FROM MYSQL.USER WHERE USER HOST='%s' AND USER='%s'", host, user))
	if err := row.Scan(&plugin); err != nil {
		return err
	}
	switch plugin {
	case "":
		alterUserSQL = fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s'", username, plugin)
	case "mysql_native_password":
		alterUserSQL = fmt.Sprintf("CREATE USER %s IDENTIFIED WITH %s BY '%s'", username, plugin, password)
	}
	_, err := d.conn.Exec(alterUserSQL)
	return err
}

//给一个用户进行赋权
//privs:ALL,CREATE,CREATE ROLE,CREATE ROUTINE,DROP,DELETE等
//objectType:TABLE,FUNCTION,PROCEDURE
//privLevel:*,*.*,db_name.*,db_name.tbl_name,tbl_name,db_name.routine_name
func (d *DBHandler) GrantUser(user, host string, privs []string, objectType, privLevel string, grantOption bool) error {
	username := userName(user, host)
	var grantUserSQL string
	if grantOption {
		grantUserSQL = fmt.Sprintf("GRANT %s ON %s %s TO %s", strings.Join(privs, ","), objectType, privLevel, username)
	} else {
		grantUserSQL = fmt.Sprintf("GRANT %s ON %s %s TO %s WITH GRANT OPTION", strings.Join(privs, ","), objectType, privLevel, username)
	}
	_, err := d.conn.Exec(grantUserSQL)
	return err
}

//复制用户
//mysql> show create user 'test1';
//+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
//| CREATE USER for test1@%                                                                                                                                                                                                                                      |
//+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
//| CREATE USER 'test1'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK PASSWORD HISTORY DEFAULT PASSWORD REUSE INTERVAL DEFAULT PASSWORD REQUIRE CURRENT DEFAULT |
//+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
//1 row in set (0.00 sec)
//mysql> SHOW GRANTS FOR 'u1'@'localhost';
//+---------------------------------------------+
//| Grants for u1@localhost                     |
//+---------------------------------------------+
//| GRANT USAGE ON *.* TO `u1`@`localhost`      |
//| GRANT `r1`@`%`,`r2`@`%` TO `u1`@`localhost` |

func (d *DBHandler) CopyUser(fromUser, fromHost, toUser, toHost string) error {
	fromUserName := userName(fromUser, fromHost)
	toUserName := userName(toUser, toHost)
	var (
		showCreatUserSQL     string
		showCreateUserResult string
		showGrantUserSQL     string
		showGrantUserResult  string
		copyOnlyUserSQL      string
		copyOnlyPrivSQL      string
	)
	//因为需要用到两个SQL语句在一个事务中，因此需要开启事务
	tx, err := d.conn.Begin()
	if err != nil {
		return err
	}
	//进行copy用户
	showCreatUserSQL = fmt.Sprintf("show create user %s", fromUserName)
	//CREATE USER 'test1'@'%' IDENTIFIED WITH xxxx
	row := tx.QueryRow(showCreatUserSQL)
	if err := row.Scan(&showCreateUserResult); err != nil {
		return err
	}
	userSubMatch := regexp.MustCompile(`CREATE USER (?P<uname>\S+) IDENTIFIED WITH`).FindStringSubmatch(showCreateUserResult)
	if len(userSubMatch) == 0 {
		return errors.New("无法匹配用户" + showCreateUserResult)
	}
	copyOnlyUserSQL = strings.Replace(showCreateUserResult, userSubMatch[1], toUserName, 1)

	//进行copy权限
	showGrantUserSQL = fmt.Sprintf("show grants for %s", fromUserName)
	row = tx.QueryRow(showGrantUserSQL)
	if err := row.Scan(&showGrantUserResult); err != nil {
		return err
	}
	grantSubMatch := regexp.MustCompile(`GRANT .+ ON .+ TO (?P<uname>\S+)`).FindStringSubmatch(showGrantUserResult)
	if len(grantSubMatch) == 0 {
		return errors.New("无法匹配权限" + showGrantUserResult)
	}
	copyOnlyPrivSQL = strings.Replace(showGrantUserResult, grantSubMatch[1], toUserName, -1)

	//开始进行创建用户并且赋权
	_, err = tx.Exec(copyOnlyUserSQL)
	if err != nil {
		return err
	}
	_, err = tx.Exec(copyOnlyPrivSQL)
	if err != nil {
		return err
	}
	return tx.Commit()
}

//获取所有的状态信息
func (d *DBHandler) ShowGlobalStatus() (map[string]string, error) {
	var (
		statusMap = make(map[string]string, 0)
		k         string
		v         string
		rows, err = d.conn.Query("show global status")
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		statusMap[k] = v
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	return statusMap, nil
}

//获取所有的参数信息
func (d *DBHandler) ShowVariables() (map[string]string, error) {
	var (
		varMap    = make(map[string]string, 0)
		k         string
		v         string
		rows, err = d.conn.Query("show variables")
	)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		varMap[k] = v
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	return varMap, nil
}

//修改数据库参数
func (d *DBHandler) SetVariable(varName, varValue string) error {
	if _, err := d.conn.Exec(fmt.Sprintf("set global %s = %s", varName, varValue)); err != nil {
		return err
	} else {
		return nil
	}
}

//查看主从复制状态
func (d *DBHandler) ShowSlaveStatus() ([]map[string]string, error) {
	showSlaveStatusSQL := "show slave status"
	rows, err := d.conn.Query(showSlaveStatusSQL)
	if err != nil {
		return nil, err
	}
	var (
		slaveMaps    []map[string]string
		colNames     []string
		colValues    []interface{}
		colValuesPtr []interface{}
	)
	slaveMaps = make([]map[string]string, 0)
	if colNames, err = rows.Columns(); err != nil {
		return nil, err
	}
	colValues = make([]interface{}, len(colNames))
	colValuesPtr = make([]interface{}, len(colNames))
	for i, _ := range colValues {
		colValuesPtr[i] = &colValues[i]
	}
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.Scan(colValuesPtr...); err != nil {
			return nil, err
		}
		slaveMap := make(map[string]string, 0)
		for i, v := range colValues {
			switch t := v.(type) {
			case []byte:
				slaveMap[colNames[i]] = string(t)
			case time.Time:
				slaveMap[colNames[i]] = t.String()
			case nil:
				slaveMap[colNames[i]] = ""
			default:
				return nil, errors.New("无法解析该数据类型" + fmt.Sprint(t))
			}

		}
		slaveMaps = append(slaveMaps, slaveMap)
	}
	rows.Close()
	return slaveMaps, nil
}

//杀掉指定session id的连接
func (d *DBHandler) KillSessionById(sessionId int) error {
	_, err := d.conn.Exec(fmt.Sprintf("kill %d", sessionId))

	return err
}

//杀掉某一个用户下的所有session id连接
func (d *DBHandler) KillSessionByUser(user string) error {
	return d.killSessionByxx("user", user, "=")
}

//杀掉某一个客户端主机名下的所有session id连接
func (d *DBHandler) KillSessionByClientHost(host string) error {
	host = host + ":%"
	return d.killSessionByxx("host", host, "like")
}

//todo 杀掉所有的查询语句
func (d *DBHandler) KillSessionBySelect() error {
	return nil
}

//byType=user,host,db,command,time,state,info
//opera= like <,<=,>,>=,!=,=
func (d *DBHandler) killSessionByxx(byType string, value interface{}, opera string) error {
	getProcesslistSQL := fmt.Sprintf("select id,user,host,db,command,time,state,info from information_schema.processlist where %s %s ?", byType, opera)
	p, err := d.conn.Prepare(getProcesslistSQL)
	if err != nil {
		return err
	}
	var (
		sessionId          int
		user               string
		host               sql.NullString
		db                 sql.NullString
		command            string
		timeSpend          string
		state              sql.NullString
		info               sql.NullString
		sessionIds         []int
		unKilledSessionIds []int
	)
	rows, err := p.Query(value)
	if err != nil {
		return err
	}
	for rows.Next() {
		if err := rows.Scan(&sessionId, &user, &host, &db, &command, &timeSpend, &state, &info); err != nil {
			return err
		}
		sessionIds = append(sessionIds, sessionId)
	}
	rows.Columns()
	p.Close()
	//开始删除会话
	for _, id := range sessionIds {
		_, err := d.conn.Exec(fmt.Sprintf("kill %d", id))
		if err != nil {
			unKilledSessionIds = append(unKilledSessionIds, id)
		}
	}
	if len(unKilledSessionIds) == 0 {
		return nil
	} else {
		return errors.New(fmt.Sprintf("Need kill sessions:%v,unkilled sessions:%v", sessionIds, unKilledSessionIds))
	}
}
