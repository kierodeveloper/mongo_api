from elasticsearch import Elasticsearch
from json import dumps
def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': '172.17.0.2', 'port': 9200}])
    if _es.ping():
        return(_es)
    else:
        return('Awww it could not connect!')
    return _es

# POST /products_test22/_analyze
analice = {
  "field": "Titulo",
  "text": "Camion"
}

# POST /products_test2/_search
search = {
  "query": {
    "match": {
      "titulo": "Batería"
    }
  }
}


# PUT /products_test22
settings = {
  "settings" : {
    "analysis" : {
      "analyzer" : {
        "my-asciifolding" : {
          "tokenizer" : "standard",
          "filter" : ["lowercase", "asciifolding"]
        }
      }
    }
  },
  "mappings": {

      "properties": {
        "Titulo": {
          "type": "text",
          "analyzer": "my-asciifolding"
        }
      }

  }
}


settings2 = {
  "settings": {
    "analysis": {
      "analyzer": {
        "folding": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "asciifolding"
          ]
        }
      }
    }
  },
  "mappings": {
      "properties": {
        "text": {
          "type": "text",
          "fields": {
            "Titulo": {
              "type": "text",
              "analyzer": "folding"
            }
          }
        }
    }
  }
}


res=connect_elasticsearch().indices.create(index='productos_oct10',body=dumps(settings2))
print(res)
#res = connect_elasticsearch().indices.analyze(index="productos_sep30", body=dumps(analice))
#print(res)from elasticsearch import Elasticsearch
from json import dumps
def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': '172.17.0.3', 'port': 9200}])
    if _es.ping():
        return(_es)
    else:
        return('Awww it could not connect!')
    return _es

# POST /products_test22/_analyze
analice = {
  "field": "Titulo",
  "text": "Camion"
}

# POST /products_test2/_search
search = {
  "query": {
    "match": {
      "titulo": "Batería"
    }
  }
}


# PUT /products_test22
settings = {
  "settings" : {
    "analysis" : {
      "analyzer" : {
        "my-asciifolding" : {
          "tokenizer" : "standard",
          "filter" : ["lowercase", "asciifolding"]
        }
      }
    }
  },
  "mappings": {

      "properties": {
        "Titulo": {
          "type": "text",
          "analyzer": "my-asciifolding"
        }
      }

  }
}


settings2 = {
  "settings": {
    "analysis": {
      "analyzer": {
        "folding": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "asciifolding"
          ]
        }
      }
    }
  },
  "mappings": {
      "properties": {
        "text": {
          "type": "text",
          "fields": {
            "Titulo": {
              "type": "text",
              "analyzer": "folding"
            }
          }
        }
    }
  }
}


res=connect_elasticsearch().indices.create(index='productos_oct10',body=dumps(settings2))
print(res)
#res = connect_elasticsearch().indices.analyze(index="productos_sep30", body=dumps(analice))
#print(res)