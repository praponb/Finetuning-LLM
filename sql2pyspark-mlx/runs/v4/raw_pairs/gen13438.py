from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("coupon_codes_count", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("k").orderBy("coupon_codes_count", "id", "k")
w_hist_2 = Window.partitionBy("k").orderBy("k_length", "id", "k").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("k").orderBy(F.col("coupon_codes_count").desc(), "id", "k")
w_ordered = Window.partitionBy("k")
w6 = Window.partitionBy("k").orderBy(F.col("seq_no").desc(), "id", "k")
w_running = Window.partitionBy("k").orderBy(F.col("coupon_codes_count").asc_nulls_first())
w8 = Window.orderBy(F.col("k_sum_k_length").asc_nulls_last())
w_running_2 = Window.orderBy("latest_rank", "id", "k")
w10 = Window.orderBy("first_known_coupon_codes_count", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "k",
        F.when(F.col("k").isin(["weight_kg", "utm_source"]), "Tier 1").otherwise("Other").alias("k_group"),
    )
)

valid = (
    filtered
    .filter(~F.col("k").isin(["gift", "device"]))
    .select(
        "*",
        F.length("k").alias("k_length"),
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.first("coupon_codes_count", ignorenulls=True).over(w).alias("first_known_coupon_codes_count"),
        F.lead("k_length", 1).over(w_hist).alias("next_k_length"),
        F.avg("coupon_codes_count").over(w_hist_2).alias("coupon_codes_count_avg_6"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("k_length").over(w_ordered).alias("k_sum_k_length"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .withColumn("coupon_codes_count_dense_rank", F.dense_rank().over(w_running))
    .withColumn("k_sum_k_length_cume_dist", F.cume_dist().over(w8))
    .withColumn("prev_seq_no", F.lag("seq_no", 2).over(w_running_2))
    .withColumn("running_k_length", F.sum("k_length").over(w10))
)

stats = (
    scored
    .groupBy("prev_seq_no")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("k") == "gift", F.col("k_length")).otherwise(0)).alias("gift_k_length"),
    )
)

result = stats.select("prev_seq_no", "gift_k_length")
