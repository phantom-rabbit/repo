package elastic

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

var es *Elastic
var ctx = context.Background()

func init() {
	var err error
	es, err = InitES("http://192.168.0.55:9200")
	if err != nil {
		panic(err)
	}
}

type Message struct {
	Cid   string
	From  string
	To    string
	Value float64
}

func TestElastic_CreateIndex(t *testing.T) {

}

func TestElastic_Del(t *testing.T) {
}

func TestElastic_DelIndex(t *testing.T) {
}

func TestElastic_Put(t *testing.T) {
	var msg = Message{
		Cid:   "0x1djsahdsaljkfljlsadkslakdla1",
		From:  "abc",
		To:    "efg",
		Value: 199.102020201,
	}
	ok := es.Put(ctx, "message", "all", msg.Cid, msg)
	fmt.Print(ok)
}

func TestElastic_QueryString(t *testing.T) {
	var msg = Message{
		Cid:   "0x1djsahdsaljkfljlsadkslakdla",
		From:  "abc",
		//To:    "efg",
		//Value: 199.102020201,
	}
	err := es.QueryOne("message", "all", "From:"+msg.From, &msg)
	require.NoError(t, err)
	fmt.Println()

	fmt.Println(msg)
}