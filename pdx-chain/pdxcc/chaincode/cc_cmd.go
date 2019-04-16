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
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"pdx-chain/pdxcc/conf"

	"pdx-chain/log"
)

func runJar(ccPath string, ccName string, port string) string {
	if ccPath == "" {
		log.Warn("ccPath is empty!!!")
		return ""
	}
	if ccName == "" {
		log.Warn("ccName is empty!!!")
		return ""
	}
	pid := isAlive(ccName)
	if pid != "" {
		log.Warn(ccName + " already started!!!")
		return ""
	}
	cpu := conf.CCViper.GetString(conf.SandboxCPU)
	memory := conf.CCViper.GetString(conf.SandboxMemory)
	argv := []string{
		conf.BaapHome + "/bin/utility/cpulimit",
		"-l",
		cpu[:len(cpu)-1],
		"--",
		"nohup",
		conf.BaapHome + "/bin/jdk1.8.0_161/jre/bin/java",
		"-Xms" + memory,
		"-Xmx" + memory,
		"-jar",
		ccPath[strings.LastIndex(ccPath, "/")+1:],
		"-a",
		"127.0.0.1:" + port,
		"-i",
		ccName,
		"-c",
		conf.ChainId.String(),
		">>",
		"/tmp/" + ccName + ".log",
		"&"}

	execCommand("cd " + ccPath[:strings.LastIndex(ccPath, "/")] + " && " + strings.Join(argv, " "))
	time.Sleep(3 * time.Second)
	return isAlive(ccName)
}

func stopJar(pid string) {
	name := "/bin/kill"
	argv := []string{"kill", "-9", pid}
	log.Warn(fmt.Sprintf("kill pid %s", pid))
	execute(name, argv)
}

func isAlive(ccName string) string {
	return execCommand(`ps -ef | grep ` + ccName + ` | grep -v grep | grep -v cpulimit | grep -v tail | awk '{print $2}'`)
}

func execute(name string, argv []string) {
	env := os.Environ()
	procAttr := &os.ProcAttr{
		Env: env,
		Files: []*os.File{
			os.Stdin,
			os.Stdout,
			os.Stderr,
		},
	}
	_, err := os.StartProcess(name, argv, procAttr)
	if err != nil {
		log.Error(fmt.Sprintf("Error %v starting process!", err)) //
		os.Exit(1)
	}
}

func execCommand(shell string) string {
	fmt.Println(shell)
	cmd := exec.Command("/bin/bash", "-c", shell)
	var stdoutBuf, stderrBuf bytes.Buffer
	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()
	var errStdout, errStderr error
	stdout := io.MultiWriter(os.Stdout, &stdoutBuf)
	stderr := io.MultiWriter(os.Stderr, &stderrBuf)
	err := cmd.Start()
	if err != nil {
		log.Error(fmt.Sprintf("cmd.Start() failed with '%s'\n", err))
	}
	go func() {
		_, errStdout = io.Copy(stdout, stdoutIn)
	}()
	go func() {
		_, errStderr = io.Copy(stderr, stderrIn)
	}()
	err = cmd.Wait()
	if err != nil {
		log.Error(fmt.Sprintf("cmd.Run() failed with %s\n", err))
	}
	if errStdout != nil || errStderr != nil {
		// fmt.Printf("failed to capture stdout or stderr\n")
	}
	outStr := string(stdoutBuf.Bytes())
	return strings.Trim(outStr, "\n")
}
