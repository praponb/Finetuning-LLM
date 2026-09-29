from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.count_if(F.col("key") == "weight_kg").alias("weight_kg_events"),
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count_if(F.col("key") == "weight_kg").alias("weight_kg_events_2"),
        F.max("created_at").alias("ended_at"),
        F.count(F.when(F.col("key") == "weight_kg", 1)).alias("weight_kg_rows"),
    )
    .filter(F.col("weight_kg_events") > 1)
    .orderBy(F.col("activity_day").desc_nulls_last())
)
