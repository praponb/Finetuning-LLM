from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("key", "attr_value"))
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
    .filter(F.col("num_keys") > 1)
    .orderBy("hour_window")
)
