from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))

result = (
    flattened
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"), "value")
    .agg(
        F.size(F.collect_set("value")).alias("num_values"),
        F.count(F.when(F.col("value") == "SPRING10", 1)).alias("spring10_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("num_values").desc())
)
