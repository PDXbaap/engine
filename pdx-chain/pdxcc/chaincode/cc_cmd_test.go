/*************************************************************************
 * Copyright (C) 2016-2019 PDX Technologies, Inc. All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *************************************************************************/
package chaincode

import (
	"encoding/json"
	"fmt"
	ioutil "io/ioutil"
	"os"
	"os/exec"
	"testing"
)

func TestRunCc(t *testing.T) {
	pid := runJar("/home/harrison/code/baapSDK/baap-sample/target/mycc-jar-with-dependencies.jar", "mycc:v39.0", "9052")
	fmt.Println(pid)
}

func TestStopCc(t *testing.T) {
	stopJar("9115")
}

func TestCc(t *testing.T) {
	fmt.Println("=====" + isAlive("mycc:v39.0"))
}

func TestRunJar(t *testing.T) {
	name := "/usr/bin/cpulimit"
	argv := []string{"cpulimit", "-l", "10", "--", "nohup", "java", "-Xms64m", "-Xmx64m", "-jar", "/home/harrison/code/baapSDK/baap-sample/target/mycc-jar-with-dependencies.jar", "-a", "127.0.0.1:9052", "-i", "mycc:v2.0", ">>", "/tmp/mycc-jar-with-dependencies.log", "&"}
	executeCmd(name, argv)
}

func TestStopJar(t *testing.T) {
	name := "/bin/kill"
	argv := []string{"kill", "9919"}
	executeCmd(name, argv)
}

func TestQuery(t *testing.T) {
	chaincodeInfos := Query("03252689c73410ea6cb0833484f6d2836b931a0ea291e8db8fe7bd07a4f1550d76")
	res, _ := json.Marshal(chaincodeInfos)
	fmt.Println(string(res))
}

func TestIsAlive(t *testing.T) {
	//name := "/bin/ps"
	//argv := []string{"ps -ef | grep mycc-jar-with-dependencies.jar | grep -v grep | awk '{print $2}'"}
	//executeCmd(name, argv)

	//cmd := exec.Command("ps -ef | grep mycc-jar-with-dependencies.jar | grep -v grep | awk '{print $2}'")
	//cmd := exec.Command("/bin/bash", "-c", `ps -ef | grep mycc:v34.0 | grep -v grep | grep -v cpulimit | awk '{print $2}'`)
	cmd := exec.Command("/bin/bash", "-c", `ps -ef | grep mycc:v34.0 | grep -v grep | grep -v cpulimit | awk '{print $2}'`)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println("StdoutPipe: " + err.Error())
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Println("StderrPipe: ", err.Error())
		return
	}

	if err := cmd.Start(); err != nil {
		fmt.Println("Start: ", err.Error())
		return
	}

	bytesErr, err := ioutil.ReadAll(stderr)
	if err != nil {
		fmt.Println("ReadAll stderr: ", err.Error())
		return
	}

	if len(bytesErr) != 0 {
		fmt.Printf("stderr is not nil: %s", bytesErr)
		return
	}

	bytes, err := ioutil.ReadAll(stdout)
	if err != nil {
		fmt.Println("ReadAll stdout: ", err.Error())
		return
	}

	if err := cmd.Wait(); err != nil {
		fmt.Println("Wait: ", err.Error())
		return
	}

	fmt.Printf("stdout: %s", string(bytes))
}

func executeCmd(name string, argv []string) {
	env := os.Environ()
	procAttr := &os.ProcAttr{
		Env: env,
		Files: []*os.File{
			os.Stdin,
			os.Stdout,
			os.Stderr,
		},
	}
	pid, err := os.StartProcess(name, argv, procAttr)
	if err != nil {
		fmt.Printf("Error %v starting process!", err) //
		os.Exit(1)
	}
	fmt.Printf("The process id is %v", pid.Pid)
}
