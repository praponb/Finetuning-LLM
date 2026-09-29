from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
w_ordered = Window.partitionBy("store_id").orderBy("quantity", "id")
w_recent = Window.orderBy(F.col("next_quantity_str").desc(), "id")
w4 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("next_quantity_str_bucket", "id")
w6 = Window.orderBy(F.col("next_quantity_str_bucket").asc_nulls_first())
w7 = Window.orderBy("cumulative_next_quantity_str")
w8 = Window.orderBy(F.col("last_restock").desc(), "id")

enriched = (
    df_inventory
    .select(
        "id",
        "last_restock",
        F.cume_dist().over(w_part).alias("quantity_cume_dist"),
        F.lead("quantity", 1).over(w_ordered).alias("next_quantity"),
    )
)

filtered = (
    enriched
    .filter(F.col("next_quantity") < 77)
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

filtered_inventory = (
    filtered
    .filter((F.col("last_restock") > F.lit("2023-07-12").cast("date")) & F.col("last_restock_month_end").between("2024-12-01", "2025-10-05"))
    .select(
        "id",
        "last_restock",
        "quantity_cume_dist",
        "last_restock_month_end",
        F.date_add("last_restock", 53).alias("last_restock_plus_53d"),
        F.col("next_quantity").cast("double").alias("next_quantity_str"),
    )
)

metrics = (
    filtered_inventory
    .select(
        "id",
        "last_restock",
        "last_restock_plus_53d",
        F.ntile(10).over(w_recent).alias("next_quantity_str_bucket"),
        F.sum("next_quantity_str").over(w4).alias("cumulative_next_quantity_str"),
    )
)

scored = (
    metrics
    .filter(F.col("last_restock_plus_53d").between("2023-06-01", "2025-05-05"))
    .select(
        "id",
        "last_restock",
        "last_restock_plus_53d",
        "next_quantity_str_bucket",
        "cumulative_next_quantity_str",
        F.ntile(4).over(w5).alias("next_quantity_str_bucket_bucket"),
        F.dense_rank().over(w6).alias("next_quantity_str_bucket_dense_rank"),
        F.cume_dist().over(w7).alias("cumulative_next_quantity_str_cume_dist"),
        F.first("cumulative_next_quantity_str", ignorenulls=True).over(w4).alias("first_known_cumulative_next_quantity_str"),
        F.row_number().over(w8).alias("seq_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "last_restock",
        "last_restock_plus_53d",
        "next_quantity_str_bucket",
        "next_quantity_str_bucket_bucket",
        "cumulative_next_quantity_str_cume_dist",
    )
    .orderBy("cumulative_next_quantity_str_cume_dist")
)
