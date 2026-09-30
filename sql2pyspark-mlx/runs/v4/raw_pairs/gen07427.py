from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("h2_events").desc())

lines = (
    df_app_logs
    .select("id", "host", "level", F.explode("context").alias("attr_key", "value"))
)

result = (
    lines
    .groupBy("attr_key", "host")
    .agg(
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
    .withColumn("h2_events_rank", F.rank().over(w))
    .orderBy(F.col("num_attr_keys").asc_nulls_first())
)
