import requests

def ingest_api(spark, src, dbutils):
    headers = {}
    if src.get("auth_type") == "bearer":
        token = dbutils.secrets.get(scope="api-secrets", key=src["token_secret"])
        headers = {"Authorization": f"Bearer {token}"}
    elif src.get("auth_type") == "apikey":
        key = dbutils.secrets.get(scope="api-secrets", key=src["token_secret"])
        headers = {"x-api-key": key}

    all_data = []
    pagination = src.get("pagination", {})

    if pagination:
        for p in range(pagination.get("start", 1), pagination.get("max_pages", 1) + 1):
            r = requests.get(src["api_url"], headers=headers, params={pagination['param']: p})
            if r.status_code == 200:
                all_data.extend(r.json())
    else:
        r = requests.get(src["api_url"], headers=headers)
        if r.status_code == 200:
            all_data = r.json()

    return spark.createDataFrame(all_data)
