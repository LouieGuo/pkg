package main

import (
	"context"
	"gitee.com/guolianyu/pkg/elasticsearch/v2"
	"strconv"
)

func InitES() {
	//初始化ES
	err := elasticsearch.InitClient(elasticsearch.DefaultClient, []string{"http://192.168.161.130:9200"},
		"elastic", "imoocpwd")
	if err != nil {
		elasticsearch.EStdLogger.Print("InitClient error", err, "client", elasticsearch.DefaultClient)
		panic(err)
	}
}
func main() {
	InitES()
	//获取elastic 客户端进行操作
	ctx := context.Background()
	esClient := elasticsearch.GetClient(elasticsearch.DefaultClient)
	//err := esClient.CreateIndex(ctx, "goods", indexCreateJson, false)
	//if err != nil {
	//	elasticsearch.EStdLogger.Print(err)
	//}

	//往索引中加入数据
	doc := Goods{
		Id:             1,
		Name:           "name1",
		Price:          1000,
		Year:           2022,
		LastMonthSales: 22,
		Favorites:      1939,
	}
	id := strconv.FormatInt(doc.Id, 10)
	err := esClient.Create(ctx, "goods", id, "", doc)
	if err != nil {
		elasticsearch.EStdLogger.Print(err)
	}

	//err := esClient.Update(ctx, IndexName, id, "", map[string]interface{}{"name": "name2"})
	//if err != nil {
	//	es.EStdLogger.Print(err)
	//}
	//doc.Name = "name4"
	//err := esClient.UpsertWithVersion(ctx, IndexName, id, "", doc, 3)
	//if err != nil {
	//	es.EStdLogger.Print(err)
	//}

	//err = esClient.Upsert(ctx, IndexName, id, "", map[string]interface{}{"name": "name4"}, doc)
	//if err != nil {
	//	es.EStdLogger.Print(err)
	//}
	//err = esClient.DeleteWithVersion(ctx, IndexName, id, "", 5)
	//if err != nil {
	//	es.EStdLogger.Print(err)
	//}
	/*
			for i := 0; i < 10; i++ {
				docID := strconv.Itoa(i)
				//update := map[string]interface{}{"name": "xxx"}
				doc := Goods{
					Id:             int64(i),
					Name:           "name" + docID,
					Price:          float64(i),
					Year:           2022,
					LastMonthSales: i,
					Favorites:      i,
				}
				//esClient.BulkUpsert(IndexName, docID, docID, update, doc)
				//esClient.BulkCreate(IndexName, docID, docID, doc)
				esClient.BulkReplace(IndexName, docID, docID, doc)
			}

		//因为是异步处理，这里需要等待本地channel提交
		time.Sleep(3 * time.Second)
	*/

	/**
	//UpdateByQuery

	updateScript := `ctx._source.name=params.name;ctx._source.favorites=params.favorites`
	//注意：map中的键必须和updateScript中params.后面的字段名一一对应，大小写及命名方式要完全一致
	updateParams := map[string]interface{}{"name": "name123", "favorites": 123}
	_, err := esClient.UpdateByQuery(ctx, IndexName, []string{"1", "2", "3"}, elastic.NewRangeQuery("id").Lte(3), updateScript, updateParams)
	if err != nil {
		es.EStdLogger.Print(err)
	}

	*/

	/*
		goods := make([]Goods, 0)
		res, err := esClient.Query(ctx, IndexName, nil, elastic.NewMatchAllQuery(), 0, 20, es.WithEnableDSL(true), es.WithOrders(map[string]bool{"favorites": true}))
		if err != nil {
			es.EStdLogger.Print(err)
		} else {
			if res != nil {
				for _, hit := range res.Hits.Hits {
					g := Goods{}
					docByte, err := hit.Source.MarshalJSON()
					if err != nil {
						es.EStdLogger.Print(err)
					} else {
						err = json.Unmarshal(docByte, &g)
						if err != nil {
							es.EStdLogger.Print(err)
						} else {
							goods = append(goods, g)
						}
					}
				}
			}

		}

		es.EStdLogger.Printf("%+v", goods)
	*/
}

type Goods struct {
	Id             int64   `json:"id"`
	Name           string  `json:"name"`
	Price          float64 `json:"price"`
	Year           int     `json:"year"`
	LastMonthSales int     `json:"last_month_sales"`
	Favorites      int     `json:"favorites"`
}

var indexCreateJson = `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "id": {
        "type": "keyword",
        "doc_values": false,
        "norms": false,
        "similarity": "boolean"
      },
      "name": {
        "type": "text"
      },
        "price": {
        "type": "double"
      },
      "last_month_sales": {
        "type": "long"
      },
      "favorites": {
        "type": "long"
      },
      "year":{
        "type": "short"
      }
    }
  }
}
`
