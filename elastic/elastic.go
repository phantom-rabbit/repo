package elastic

import (
	"github.com/olivere/elastic"
	"gopkg.in/olivere/elastic.v6"
)

type Elastic struct {
	Client *elastic.Client
	host   string
}


