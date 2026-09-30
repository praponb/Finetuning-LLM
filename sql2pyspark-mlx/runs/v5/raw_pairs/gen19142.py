from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("quantity").desc())
w_recent = Window.orderBy("quantity_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("quantity_num").desc_nulls_first())

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-03-16").cast("date")) & F.col("product_id").isin([1, 4, 7, 8]))
    .select("*", F.col("quantity").cast("double").alias("quantity_num"))
)

metrics = (
    cleaned
    .select(
        "id",
        "last_restock",
        "quantity_num",
        F.count("quantity_num").over(w).alias("running_count_quantity_num"),
        F.avg("quantity_num").over(w).alias("running_avg_quantity_num"),
        F.max("quantity_num").over(w).alias("running_max_quantity_num"),
        F.rank().over(w2).alias("quantity_rank"),
    )
)

cleaned_inventory = (
    metrics
    .filter(F.col("last_restock") >= F.lit("2023-06-23").cast("date"))
    .select(
        "*",
        F.round("running_max_quantity_num", 0).alias("running_max_quantity_num_rounded"),
    )
)

enriched = (
    cleaned_inventory
    .filter(F.col("running_avg_quantity_num").isNotNull())
    .select(
        "id",
        "last_restock",
        "quantity_num",
        "running_count_quantity_num",
        "running_avg_quantity_num",
        "running_max_quantity_num_rounded",
        F.sum("quantity_num").over(w_recent).alias("running_quantity_num"),
        F.first("quantity_rank", ignorenulls=True).over(w_recent).alias("first_known_quantity_rank"),
        F.rank().over(w4).alias("quantity_num_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "quantity_num",
        "running_count_quantity_num",
        "running_avg_quantity_num",
        "running_max_quantity_num_rounded",
        "running_quantity_num",
        "first_known_quantity_rank",
    )
    .orderBy(F.col("first_known_quantity_rank").desc_nulls_last())
)
