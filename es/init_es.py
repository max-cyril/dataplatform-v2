from elasticsearch import Elasticsearch, exceptions
import time
import sys

es = Elasticsearch("http://elasticsearch:9200")

MAPPING = {
    "mappings": {
        "properties": {
            "timestamp":   { "type": "date" },
            "level":       { "type": "keyword" },
            "message":     { "type": "text" },
            "script":      { "type": "keyword" },
            "duration_ms": { "type": "float" }
        }
    }
}

INDEXES = ["records", "streams", "ingest_raw"]

def create_index(index_name, mapping):
    try:
        if es.indices.exists(index=index_name):
            print(f"Index '{index_name}' existe déjà.")
            return 
            
        response = es.indices.create(
            index=index_name,
            body=mapping,
            ignore=400  # Ignore les erreurs 400 (déjà existant)
        )
        
        if 'acknowledged' in response and response['acknowledged']:
            print(f"✅ Index '{index_name}' créé.")
        else:
            print(f"⚠️ Réponse inattendue: {response}")
            
    except Exception as e:
        print(f"❌ Erreur lors de la création de l'index {index_name}: {str(e)}")
        raise

def run():
    time.sleep(10)

    for index in INDEXES:
        create_index(index, MAPPING)
        print(f"✅ Elasticsearch index '{index}' créé.")

if __name__ == "__main__":
    run()