from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("utm_source_count").desc())

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .groupBy("attr_key", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("attr_key") == "utm_source", 1)).alias("utm_source_count"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .withColumn("utm_source_count_dense_rank", F.dense_rank().over(w))
)
