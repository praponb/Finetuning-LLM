from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value", "customer_id")
w2 = Window.orderBy(F.col("value_length").asc_nulls_last())
w3 = Window.orderBy(F.col("value_length").desc_nulls_first())
w4 = Window.orderBy(F.col("value_length_adjusted").desc_nulls_first())
w5 = Window.partitionBy("customer_id")

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("value"))

base = (
    exploded
    .filter(F.col("value").isin(["WELCOME5", "BUNDLE", "SPRING10"]))
    .withColumn("value_length", F.length("value"))
    .withColumn("value_filled", F.coalesce("value", F.lit("N/A")))
    .withColumn("value_filled_2", F.coalesce("value", F.lit("NONE")))
)

filtered = (
    base
    .filter(F.col("value") == "SPRING10")
    .select(
        "*",
        F.round((F.col("value_length") * 1.1), 2).alias("value_length_adjusted"),
        F.coalesce("value", F.lit("NONE")).alias("value_clean"),
    )
)

scored = (
    filtered
    .filter(F.col("value_length") != 43)
    .withColumn("value_min_value_length_adjusted", F.min("value_length_adjusted").over(w))
    .withColumn("value_length_percent_rank", F.percent_rank().over(w2))
    .withColumn("value_length_rank", F.rank().over(w3))
    .withColumn("value_length_adjusted_rank", F.rank().over(w4))
    .withColumn("customer_id_max_value_length", F.max("value_length").over(w5))
)

wide = (
    scored
    .groupBy("customer_id")
    .pivot("value", ["BUNDLE", "SPRING10"])
    .agg(
        F.sum("value_length_adjusted_rank").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)

result = (
    wide
    .select(
        "customer_id",
        "BUNDLE_value_sum",
        "BUNDLE_item_count",
        "SPRING10_value_sum",
    )
    .orderBy(F.col("SPRING10_value_sum").asc_nulls_first())
)
