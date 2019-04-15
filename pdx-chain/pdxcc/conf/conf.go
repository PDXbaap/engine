package conf

import (
	"bufio"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"pdx-chain/log"
	"time"

	"github.com/spf13/viper"
	"pdx-chain/core/state"
)

var BaapHome = os.Getenv("PDX_BAAP_HOME")
var DbPath = BaapHome + ""

var ChainId *big.Int

var GetStateDB func() (*state.StateDB, error)

var NoLimitCC = map[string][]byte{
	"baap-deploy:v1.0":    {0},
	"baap-stream:v1.0":    {0},
	"baap-payment:v1.0":   {0},
	"baap-chainiaas:v1.0": {0},
	"baap-trusttree:v1.0": {0},
}

const (
	MaxResultSize  = 100
	Space          = "\x00"
	HisKeyTemplate = "%s-baap-his"
	RangeKey       = "baap-range"
)

const (
	SetETH     = "set"
	SetPDX     = "setPDX"
	GetETH     = "get"
	GetPDX     = "getPDX"
	ETHKeyFlag = "1"
	PDXKeyFlag = "2"
)

var (
	ApplyTime  = time.Millisecond * 2000
)

//noinspection ALL
const (
	//Utopia-filled TX meta data, readable by chaincode
	BaapEngineID = "baap-stack-id"
	BaapDst      = "baap-dst"
	BaapSpbk     = "baap-public-key"
	BaapTxid     = "baap-txid"
	BaapTxType = "baap-tx-type"

	BaapEnable               = "pdxc.baap.enable"
	BaapSandboxPolicy        = "pdxc.baap.sandbox.policy"
	BaapConfPath             = "/conf/baap.properties"
	BaapCertPath             = "/conf/client.crt"
	BaapCaPath               = "/conf/root.crt"
	BaapSandboxPolicyDefault = "PUBLIC-MOST-RESTRICTED"
	StreamKey                = "pdxc.baap.stream"

)

const (
	SandboxCCPolicyFile  = "cc-profile"
	SandboxJrePolicyFile = "jre.policy"
	SandboxPolicyDIR     = "/conf/sandbox-policy/"
	SandboxStateKeyNum   = "state.key.num"
	SandboxStateHisNum   = "state.his.num"
	SandboxStateSize     = "state.size"
	SandboxCPU           = "cpu"
	SandboxMemory        = "memory"
	SandboxNet           = "net"
	SandboxHdd           = "hdd"
)

var BaapViper = viper.New()
var CCViper = viper.New()

var NetWorkId int

var SandboxCCPolicyPath string
var SandboxJrePolicyPath string

const (
	Exec  = "exec"
	Init  = "init"
	Query = "query"
)

func InitConf() {
	fi, err := os.Open(BaapHome + BaapConfPath)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	// baap enable
	BaapViper.SetConfigType("properties")
	BaapViper.ReadConfig(bufio.NewReader(fi))
	log.Info("BaapEnable", "enable", BaapViper.GetBool(BaapEnable))

	// baap sandbox
	sandboxPolicy := BaapViper.GetString(BaapSandboxPolicy)
	if sandboxPolicy == "" {
		sandboxPolicy = BaapSandboxPolicyDefault
	}

	SandboxCCPolicyPath = BaapHome + SandboxPolicyDIR + sandboxPolicy + "/" + SandboxCCPolicyFile
	SandboxJrePolicyPath = BaapHome + SandboxPolicyDIR + sandboxPolicy + "/" + SandboxJrePolicyFile

	fi, err = os.Open(SandboxCCPolicyPath)
	if err != nil {
		panic(err)
	}
	defer fi.Close()
	CCViper.SetConfigType("json")
	CCViper.ReadConfig(bufio.NewReader(fi))

	log.Info("SandboxStateKeyNum", "num", CCViper.GetInt(SandboxStateKeyNum))

	pool := x509.NewCertPool()
	addTrust(pool, BaapHome+BaapCaPath)
}

func addTrust(pool *x509.CertPool, path string) {
	aCrt, err := ioutil.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("ReadFile err: %s", err.Error()))
	}
	pool.AppendCertsFromPEM(aCrt)
}
