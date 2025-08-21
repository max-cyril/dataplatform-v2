import time
import uuid
import json
import requests
from functools import wraps
from datetime import datetime

ELASTIC_URL = "http://elasticsearch:9200"

def log_to_es(index):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log_id = str(uuid.uuid4())
            start = time.time()
            status = "success"
            error_msg = None

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                status = "error"
                error_msg = str(e)
                raise
            finally:
                end = time.time()
                duration = round(end - start, 3)
                log_doc = {
                    "id": log_id,
                    "function": func.__name__,
                    "status": status,
                    "duration_sec": duration,
                    "timestamp": datetime.utcnow().isoformat(),
                    "error": error_msg,
                    "metadata": {
                        "items": kwargs.get("items")
                    }
                    #{"args": str(args),
                    #   "kwargs": str(kwargs)}
                }

                try:
                    response = requests.post(
                        f"{ELASTIC_URL}/{index}/_doc",
                        headers={"Content-Type": "application/json"},
                        data=json.dumps(log_doc)
                    )
                    if response.status_code >= 300:
                        print(f"❌ Failed to send log to Elasticsearch: {response.text}")
                except Exception as e:
                    print(f"❌ Exception sending log to Elasticsearch: {str(e)}")

        return wrapper
    return decorator
