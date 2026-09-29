from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w_running = Window.partitionBy("store_id").orderBy("quantity_whole", "id")
w3 = Window.orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("quantity_whole", "id")
w5 = Window.orderBy("prev_prev_quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-21").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "last_restock", "quantity_whole")
)

with_windows = (
    ranked
    .filter(F.month("last_restock").isin([6, 9, 10]))
    .select(
        "id",
        "quantity_whole",
        F.lag("quantity_whole").over(w_running).alias("prev_quantity_whole"),
        F.lead("quantity_whole").over(w_running).alias("next_quantity_whole"),
        F.row_number().over(w_running).alias("seq_no"),
        F.sum("quantity_whole").over(w3).alias("running_quantity_whole"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.lag("prev_quantity_whole").over(w_grp).alias("prev_prev_quantity_whole"),
        F.lead("prev_quantity_whole").over(w_grp).alias("next_prev_quantity_whole"),
        F.lead("next_quantity_whole").over(w_grp).alias("next_next_quantity_whole"),
        F.row_number().over(w_grp).alias("seq_no_2"),
    )
)

enriched_inventory = (
    enriched
    .select(
        "id",
        "quantity_whole",
        "prev_quantity_whole",
        "next_quantity_whole",
        "seq_no",
        "running_quantity_whole",
        "next_prev_quantity_whole",
        "next_next_quantity_whole",
        "seq_no_2",
        F.max("quantity_whole").over(w5).alias("running_max_quantity_whole"),
        F.avg("quantity_whole").over(w5).alias("running_avg_quantity_whole"),
        F.min("quantity_whole").over(w5).alias("running_min_quantity_whole"),
    )
)

filtered = (
    enriched_inventory
    .filter(F.col("running_max_quantity_whole") <= 31)
    .select(
        "id",
        "quantity_whole",
        "prev_quantity_whole",
        "seq_no",
        "running_quantity_whole",
        "next_prev_quantity_whole",
        "next_next_quantity_whole",
        "seq_no_2",
        "running_max_quantity_whole",
        "running_avg_quantity_whole",
        "running_min_quantity_whole",
        (F.col("running_quantity_whole") + F.col("next_quantity_whole")).alias("running_quantity_whole_plus_next_quantity_whole"),
        F.coalesce("prev_quantity_whole", F.lit(0)).alias("prev_quantity_whole_filled"),
    )
)

summary = (
    filtered
    .groupBy("seq_no")
    .agg(
        F.avg("running_quantity_whole").alias("avg_running_quantity_whole"),
        F.percentile_approx("quantity_whole", 0.9).alias("quantity_whole_p90"),
        F.percentile_approx("prev_quantity_whole_filled", 0.5).alias("prev_quantity_whole_filled_p50"),
    )
    .filter(F.col("quantity_whole_p90") < 1000000)
)

result = (
    summary
    .select("seq_no", "avg_running_quantity_whole", "quantity_whole_p90")
    .orderBy(F.col("seq_no").asc_nulls_first())
)
