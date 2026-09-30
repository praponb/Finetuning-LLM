from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity_adjusted", "id").rowsBetween(-6, -1)
w_part = Window.orderBy("quantity_adjusted", "id")
w_recent = Window.orderBy("next_quantity_adjusted_avg_6_prior", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy(F.col("prev_running_quantity").desc(), "id")
w6 = Window.orderBy(F.col("next_quantity_adjusted_avg_6_prior").desc(), "id")
w7 = Window.orderBy("row_seq", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy("quantity_adjusted_avg_6_prior", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-09-01").cast("date")) & F.col("store_id").isin([2, 4]))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 100), 2).alias("quantity_adjusted"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "quantity_adjusted",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.avg("quantity_adjusted").over(w2).alias("quantity_adjusted_avg_6_prior"),
    )
)

with_windows_inventory = (
    with_windows
    .filter(F.col("quantity_adjusted").isNotNull())
    .select(
        "*",
        F.lag("quantity_adjusted").over(w_part).alias("prev_quantity_adjusted"),
        F.lead("quantity_adjusted").over(w_part).alias("next_quantity_adjusted"),
        F.lead("running_quantity").over(w_part).alias("next_running_quantity"),
        F.lag("running_quantity").over(w_part).alias("prev_running_quantity"),
        F.row_number().over(w_part).alias("row_seq"),
        F.lead("quantity_adjusted_avg_6_prior", 2).over(w_part).alias("next_quantity_adjusted_avg_6_prior"),
    )
)

enriched = (
    with_windows_inventory
    .withColumn("next_quantity_adjusted_avg_2", F.avg("next_quantity_adjusted").over(w_recent))
    .withColumn("prev_running_quantity_bucket", F.ntile(3).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("quantity_adjusted_avg_6", F.avg("quantity_adjusted").over(w7))
    .withColumn("quantity_adjusted_avg_6_prior_avg_2", F.avg("quantity_adjusted_avg_6_prior").over(w8))
)

grouped = (
    enriched
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("quantity_adjusted_tier", F.when(F.col("quantity_adjusted") >= 88, "HIGH").otherwise("LOW"))
    .rollup("event_seq_tier", "quantity_adjusted_tier")
    .agg(
        F.percentile_approx("prev_running_quantity_bucket", [0.25, 0.5, 0.75]).alias("prev_running_quantity_bucket_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .select("event_seq_tier", "quantity_adjusted_tier", "distinct_ids", "gid")
    .orderBy(F.col("distinct_ids").desc_nulls_last(), "event_seq_tier")
)
