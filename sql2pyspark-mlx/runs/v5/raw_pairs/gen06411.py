from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("created_at", "id", "k")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("created_at").desc(), "id", "k")

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

with_gaps = (
    flattened
    .withColumn("is_new_burst", F.when(F.col("created_at") - F.lag("created_at").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_carts = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

deduped = (
    sessions_carts
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

matched_keys = (
    deduped
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
    )
    .agg(
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("k") == "gift", 1)), F.lit(0)), 2).alias("burst_id_per_gift"),
        F.avg(F.when(F.col("k") == "gift", F.col("is_new_burst"))).alias("gift_avg_is_new_burst"),
        F.max(F.when(F.col("k") == "device", F.col("burst_id"))).alias("device_max_burst_id"),
        F.count(F.when(F.col("k") == "device", 1)).alias("device_count"),
        F.max(F.when(F.col("k") == "gift", F.col("burst_id"))).alias("gift_max_burst_id"),
    )
    .orderBy(F.col("gift_avg_is_new_burst").desc())
)
