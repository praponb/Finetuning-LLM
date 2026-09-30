from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
        F.min("created_at").alias("started_at"),
        F.count_if(F.col("key") == "weight_kg").alias("weight_kg_events"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .filter(F.col("gift_rows") >= 1)
)
