from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "service", "level", F.explode("context").alias("key", "attr_value"))
)

result = (
    flattened
    .select("id", "service", "key", "attr_value")
    .orderBy("service", "id")
)
