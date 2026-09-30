from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_order_items
    .select(
        "*",
        F.max("quantity").over(w_recent).alias("running_max_quantity"),
        F.sum("quantity").over(w_recent).alias("running_sum_quantity"),
        F.min("quantity").over(w_recent).alias("running_min_quantity"),
        F.avg("quantity").over(w_recent).alias("running_avg_quantity"),
    )
)

result = (
    enriched
    .withColumn("running_min_quantity_tier", F.when(F.col("running_min_quantity") >= 8.9, "HIGH").otherwise("LOW"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 9, "PREMIUM").otherwise("STANDARD"))
    .withColumn("running_sum_quantity_tier", F.when(F.col("running_sum_quantity") >= 40, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_min_quantity_tier", "quantity_tier", "running_sum_quantity_tier"], ["running_min_quantity_tier", "quantity_tier"], []],
        "running_min_quantity_tier",
        "quantity_tier",
        "running_sum_quantity_tier",
    )
    .agg(
        F.round(F.avg("running_min_quantity"), 2).alias("avg_running_min_quantity"),
        F.percentile_approx("running_min_quantity", [0.25, 0.5, 0.75]).alias("running_min_quantity_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
)
