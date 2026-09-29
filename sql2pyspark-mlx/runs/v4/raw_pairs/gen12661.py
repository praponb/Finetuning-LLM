from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

result = (
    exploded
    .groupBy("k")
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_rows"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("span_minutes") < 0)
)
