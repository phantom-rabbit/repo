package elastic

import (
	"context"
	"encoding/json"
	"fmt"

	"gopkg.in/olivere/elastic.v6"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("elastic")

type Elastic struct {
	Client *elastic.Client
	host   string
}

func InitES(host string) (*Elastic, error) {
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(host))
	if err != nil {
		panic(err)
	}

	info, code, err := client.Ping(host).Do(context.Background())
	if err != nil {
		panic(err)
	}
	log.Infof("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	esVersion, err := client.ElasticsearchVersion(host)
	if err != nil {
		panic(err)
	}

	log.Infof("Elasticsearch version %s\n", esVersion)
	es := &Elastic{
		Client: client,
		host: host,
	}

	return es, nil
}

func (self *Elastic)CreateIndex(ctx context.Context, index, mapping string) bool {
	exists, err := self.Client.IndexExists(index).Do(ctx)
	if err != nil {
		log.Errorf("<CreateIndex> some error occurred when check exists, index: %s, err:%s", index, err.Error())
		return false
	}

	if exists {
		log.Infof("<CreateIndex> index:{%s} is already exists", index)
		return true
	}

	createIndex, err := self.Client.CreateIndex(index).Body(mapping).Do(ctx)
	if err != nil {
		log.Errorf("<CreateIndex> some error occurred when create. index: %s, err:%s", index, err.Error())
		return false
	}
	if !createIndex.Acknowledged {
		// Not acknowledged
		log.Errorf("<CreateIndex> Not acknowledged, index: %s", index)
		return false
	}
	return true
}

func (self *Elastic)DelIndex(ctx context.Context, index string) error {
	deleteIndex, err := self.Client.DeleteIndex(index).Do(ctx)
	if err != nil {
		log.Errorf("<DelIndex> some error occurred when delete. index: %s, err:%s", index, err.Error())
		return err
	}
	if !deleteIndex.Acknowledged {
		return fmt.Errorf("<DelIndex> acknowledged. index: %s", index)
	}
	return nil
}

func (self *Elastic)Put(ctx context.Context, index, typ, id string, body interface{}) error {
	put, err := self.Client.Index().
		Index(index).
		Type(typ).
		Id(id).
		BodyJson(body).
		Do(ctx)
	if err != nil {
		log.Errorf("<Put> some error occurred when put.  err:%s", err.Error())
		return err
	}
	log.Infof("<Put> success, id: %s to index: %s, type %s\n", put.Id, put.Index, put.Type)
	return nil
}

func (self *Elastic)Del(ctx context.Context, index, typ, id string) bool {
	del, err := self.Client.Delete().
		Index(index).
		Type(typ).
		Id(id).
		Do(ctx)
	if err != nil {
		log.Errorf("<Del> some error occurred when del.  err:%s", err.Error())
		return false
	}
	log.Infof("<Del> success, id: %s to index: %s, type %s\n", del.Id, del.Index, del.Type)
	return true
}

func (self *Elastic)Update(ctx context.Context, index, typ, id string, updateMap map[string]interface{}) bool {
	res, err := self.Client.Update().
		Index(index).Type(typ).Id(id).
		Doc(updateMap).
		FetchSource(true).
		Do(ctx)
	if err != nil {
		log.Errorf("<Update> some error occurred when update. index:%s, typ:%s, id:%s err:%s", index, typ, id, err.Error())
		return false
	}
	if res == nil {
		log.Errorf("<Update> expected response != nil. index:%s, typ:%s, id:%s", index, typ, id)
		return false
	}
	if res.GetResult == nil {
		log.Errorf("<Update> expected GetResult != nil. index:%s, typ:%s, id:%s", index, typ, id)
		return false
	}

	return true
}


func (self *Elastic)QueryOne(index, typ, query string, out interface{}) error {
	q := elastic.NewQueryStringQuery(query)
	// Match all should return all documents
	searchResult, err := self.Client.Search().
		Index(index).
		Type(typ).    // type of Index
		Query(q).
		Size(1).
		Do(context.Background())
	if err != nil {
		log.Errorf("<QueryString> some error occurred when search. index:%s, query:%v,  err:%s", index, query, err.Error())
		return err
	}

	for _, hit := range searchResult.Hits.Hits {
		err := json.Unmarshal(*hit.Source, out)
		if err != nil {
			return err
		}
	}
	
	return nil
}