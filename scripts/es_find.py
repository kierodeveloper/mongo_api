# Import Elasticsearch package 
from elasticsearch import Elasticsearch 
# Connect to the elastic cluster

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Conectado')
        return(_es)
    else:
        return('Awww it could not connect!')
    return _es

_es = connect_elasticsearch()

doc = {
 "doc":{
   "Titulo":"Gafas, Zrong pet dog gafas de proteccion solar, gafas protectoras"
 }
}
res = _es.index(index='publies_products_v2', body=doc)
print(res)

# POST /search_products/_update/674179
# {
#  "doc":{
#    "Titulo":"Gafas para perro, petleso perro grande gafas de proteccion solar gafas uv gafas - negro"
#  }
# }
# #################################
# GET /search_products/_search?filter_path=hits.hits._source
# {
#   "query":{
#      "match" : {
#         "Producto_Id":"186139"
#      }
#   }
# }