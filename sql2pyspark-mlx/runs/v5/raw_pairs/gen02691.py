from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

result = (
    exploded
    .groupBy("code")
    .agg(
        F.max("created_at").alias("last_created_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_rows"),
        F.count_if(F.col("code") == "BUNDLE").alias("bundle_events"),
    )
    .filter(F.col("vip_rows") > 1)
    .orderBy(F.col("vip_rows").desc_nulls_first())
)
