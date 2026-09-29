from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value_whole").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value_int").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w4 = Window.orderBy(F.col("tier_value_int").desc_nulls_first())
w5 = Window.partitionBy("plan").orderBy("tier_value_int")

lines = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

valid = (
    lines
    .filter(F.col("tier_value") <= 71)
    .withColumn("tier_value_int", F.col("tier_value").cast("int"))
    .withColumn("tier_value_whole", F.floor("tier_value"))
)

with_windows = (
    valid
    .select(
        "*",
        F.cume_dist().over(w).alias("tier_value_whole_cume_dist"),
        F.rank().over(w2).alias("tier_value_int_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
        F.percent_rank().over(w4).alias("tier_value_int_percent_rank"),
        F.percent_rank().over(w5).alias("tier_value_int_percent_rank_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("tier_value_whole") >= 81, "HIGH").otherwise("LOW").alias("tier_value_whole_tier"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("tier_value_int_percent_rank_2"))).alias("enterprise_total_tier_value_int_percent_rank_2"),
        F.round(F.sum("tier_value_int_rank") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("tier_value_int_rank_per_enterprise"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
    .filter(F.col("basic_rows") > 1)
)
