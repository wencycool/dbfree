package utils

import (
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"net"
	"os"
	"path/filepath"
	"time"
)

//将一个文件从FTP服务器上下载到本地指定目录下
func DownloadFile(user, password, host string, port int, fileName, remoteDir, localDir string) error {
	sftpClient, err := connect(user, password, host, port)
	if err != nil {
		return err
	}
	defer sftpClient.Close()
	srcFile, err := sftpClient.Open(filepath.Join(remoteDir, fileName))
	if err != nil {
		return nil
	}
	defer srcFile.Close()
	dstFile, err := os.Create(filepath.Join(localDir, fileName))
	if err != nil {
		return err
	}
	defer dstFile.Close()
	if _, err := srcFile.WriteTo(dstFile); err != nil {
		return err
	}
	return nil
}

func connect(user, password, host string, port int) (*sftp.Client, error) {
	var (
		auth         []ssh.AuthMethod
		addr         string
		clientConfig *ssh.ClientConfig
		sshClient    *ssh.Client
		sftpClient   *sftp.Client
		err          error
	)
	// get auth method
	auth = make([]ssh.AuthMethod, 0)
	auth = append(auth, ssh.Password(password))

	clientConfig = &ssh.ClientConfig{
		User:    user,
		Auth:    auth,
		Timeout: 30 * time.Second,
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}

	// connet to ssh
	addr = fmt.Sprintf("%s:%d", host, port)

	if sshClient, err = ssh.Dial("tcp", addr, clientConfig); err != nil {
		return nil, err
	}

	// create sftp client
	if sftpClient, err = sftp.NewClient(sshClient); err != nil {
		return nil, err
	}

	return sftpClient, nil
}

//获取本地IP列表,如果包括回环地址则loopback为true
func GetLocalIPs(loopback bool) (ips []string) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	for _, value := range addrs {
		//&& !ipnet.IP.IsLoopback() --保留本地127.0.0.1回环地址
		if ipnet, ok := value.(*net.IPNet); ok && (!ipnet.IP.IsLoopback() || (ipnet.IP.IsLoopback() && loopback)) {
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP.String())
			}
		}
	}
	return ips
}

//获取本地实例信息

func GetLocalAliveInstances() ([]*MySQLInstance, error) {
	return getMySQLInstances()
}
