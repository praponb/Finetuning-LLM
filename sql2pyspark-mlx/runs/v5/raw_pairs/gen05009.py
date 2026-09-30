from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "rec"))
    .select(
        "id",
        "created_at",
        "pos",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.promo"),
    )
)

result = (
    lines
    .withColumn("pos_tier", F.when(F.col("pos") >= 2, "PREMIUM").otherwise("STANDARD"))
    .cube("pos_tier")
    .agg(
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("span_minutes")
)
