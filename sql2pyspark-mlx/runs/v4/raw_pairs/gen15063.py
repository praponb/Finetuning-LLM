from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy("value_length")
w_part = Window.partitionBy("value").orderBy(F.col("value_length").asc_nulls_last())
w_hist = Window.partitionBy("value")
w4 = Window.partitionBy("value").orderBy(F.col("value_max_value_length").asc_nulls_last())
w5 = Window.partitionBy("value").orderBy(F.col("value_max_value_max_value_length").asc_nulls_first())
w6 = Window.partitionBy("value").orderBy(F.col("value_max_value_avg_value_length_scaled").asc_nulls_first())

lines = df_carts.select("id", F.explode_outer("coupon_codes").alias("value"))

filtered = (
    lines
    .filter(F.col("value").isNotNull())
    .withColumn("value_length", F.length("value"))
)

scored = (
    filtered
    .withColumn("value_length_percent_rank", F.percent_rank().over(w))
    .withColumn("value_length_percent_rank_2", F.percent_rank().over(w_part))
    .withColumn("value_max_value_length", F.max("value_length").over(w_hist))
    .withColumn("value_avg_value_length", F.avg("value_length").over(w_hist))
)

base = (
    scored
    .filter(F.col("value_length_percent_rank") > 0.47)
    .select(
        "*",
        F.round((F.col("value_avg_value_length") * 0.85), 2).alias("value_avg_value_length_scaled"),
    )
)

with_windows = (
    base
    .withColumn("value_max_value_avg_value_length_scaled", F.max("value_avg_value_length_scaled").over(w_hist))
    .withColumn("value_max_value_max_value_length", F.max("value_max_value_length").over(w_hist))
)

valid = (
    with_windows
    .filter(F.col("value_length_percent_rank").between(0.32, 0.64))
    .select(
        "value",
        "value_length",
        "value_max_value_length",
        "value_avg_value_length",
        "value_max_value_avg_value_length_scaled",
        "value_max_value_max_value_length",
        (F.col("value_length_percent_rank") - F.col("value_avg_value_length_scaled")).alias("value_length_percent_rank_minus_value_avg_value_length_scaled"),
    )
)

enriched = (
    valid
    .withColumn("value_max_value_length_dense_rank", F.dense_rank().over(w4))
    .withColumn("value_max_value_max_value_length_cume_dist", F.cume_dist().over(w5))
    .withColumn("value_max_value_avg_value_length_scaled_dense_rank", F.dense_rank().over(w6))
    .withColumn("value_min_value_max_value_max_value_length", F.min("value_max_value_max_value_length").over(w_hist))
)

result = (
    enriched
    .select(
        "value",
        "value_length",
        "value_max_value_length",
        "value_max_value_avg_value_length_scaled",
        "value_max_value_max_value_length_cume_dist",
        "value_max_value_avg_value_length_scaled_dense_rank",
        "value_min_value_max_value_max_value_length",
        F.when(F.col("value").rlike("^(WELCOME5|SPRING10)$"), "GROUP_A").otherwise("CORE").alias("value_zone"),
    )
    .orderBy("value_length")
)
