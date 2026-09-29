from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")
w_recent = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("next_quantity", "id")
w5 = Window.orderBy("running_count_next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("running_max_next_quantity_tier").orderBy(F.col("avg_running_max_next_quantity").desc())

enriched = (
    df_inventory
    .filter(F.year("last_restock") == 2024)
    .select(
        "*",
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("row_seq"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "next_quantity", "row_seq")
)

with_windows = (
    ranked
    .select(
        "id",
        "next_quantity",
        "row_seq",
        F.max("next_quantity").over(w_recent).alias("running_max_next_quantity"),
        F.count("next_quantity").over(w_recent).alias("running_count_next_quantity"),
        F.avg("next_quantity").over(w_recent).alias("running_avg_next_quantity"),
    )
)

scored = (
    with_windows
    .withColumn("prev_running_count_next_quantity", F.lag("running_count_next_quantity").over(w4))
    .withColumn("next_running_count_next_quantity", F.lead("running_count_next_quantity").over(w4))
    .withColumn("next_next_quantity", F.lead("next_quantity").over(w4))
    .withColumn("prev_next_quantity", F.lag("next_quantity").over(w4))
    .withColumn("row_seq_2", F.row_number().over(w4))
    .withColumn("running_running_max_next_quantity", F.sum("running_max_next_quantity").over(w5))
)

cleaned = (
    scored
    .filter(F.col("running_max_next_quantity") >= 160)
    .withColumn("greatest_running_running_max_next_quantity_running_count_next_quantity", F.greatest("running_running_max_next_quantity", "running_count_next_quantity"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("running_max_next_quantity") >= 251, "LARGE").otherwise("SMALL").alias("running_max_next_quantity_tier"),
        F.when(F.col("next_quantity") >= 223, "PREMIUM").otherwise("STANDARD").alias("next_quantity_tier"),
    )
    .agg(
        F.percentile_approx("running_avg_next_quantity", [0.25, 0.5, 0.75]).alias("running_avg_next_quantity_quartiles"),
        F.round(F.avg("running_max_next_quantity"), 2).alias("avg_running_max_next_quantity"),
    )
    .withColumn("avg_running_max_next_quantity_rank", F.rank().over(w6))
    .orderBy(F.col("running_max_next_quantity_tier").desc())
)
