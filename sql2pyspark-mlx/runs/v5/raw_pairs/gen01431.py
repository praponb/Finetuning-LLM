from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_store_id = df_orders.groupBy("store_id").agg(F.min("amount").alias("min_order_amount"))

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-05-08"))) & (F.col("quantity") > 127))
    .select(
        "*",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "quantity", "last_restock")
)

metrics = (
    latest
    .select(
        "*",
        F.count("quantity").over(w2).alias("running_count_quantity"),
        F.avg("quantity").over(w2).alias("running_avg_quantity"),
        F.min("quantity").over(w2).alias("running_min_quantity"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
    )
)

cleaned_inventory = (
    metrics
    .filter(F.col("last_restock") >= F.lit("2023-04-07").cast("date"))
    .withColumn("last_restock_day", F.dayofmonth("last_restock"))
    .withColumn("running_min_quantity_whole", F.floor("running_min_quantity"))
)

summary = (
    cleaned_inventory
    .withColumn("cumulative_quantity_tier", F.when(F.col("cumulative_quantity") >= 1055, "PREMIUM").otherwise("STANDARD"))
    .rollup("store_id", "cumulative_quantity_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.grouping("store_id").alias("grouping_id"),
    )
)

result = (
    summary
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "cumulative_quantity_tier",
        "distinct_ids",
        "quantity_quartiles",
        "distinct_store_ids",
        F.col("min_order_amount"),
    )
    .orderBy(F.col("min_order_amount").desc_nulls_first(), "distinct_store_ids")
)
