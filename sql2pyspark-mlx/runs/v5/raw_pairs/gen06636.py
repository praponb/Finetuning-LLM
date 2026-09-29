from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Singapore", "New York"])
    .agg(
        F.sum("credit_limit").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "segment",
        F.col("Singapore_value_sum").alias("singapore_value_sum"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
        F.col("New York_value_sum").alias("new_york_value_sum"),
        F.col("New York_item_count").alias("new_york_item_count"),
    )
)
