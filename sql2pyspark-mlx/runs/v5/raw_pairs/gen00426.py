from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w_part = Window.orderBy(F.col("quantity").desc_nulls_last())
w_part_2 = Window.orderBy(F.col("quantity").desc(), "id")
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("last_known_quantity").desc_nulls_last())
w7 = Window.orderBy(F.col("last_known_quantity").desc_nulls_first())
w_recent = Window.orderBy("quantity_dense_rank", "id")
w9 = Window.orderBy(F.col("last_known_quantity_dense_rank").desc_nulls_first())

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-04-14").cast("date"))
    .select(
        "id",
        "quantity",
        F.date_add("last_restock", 21).alias("last_restock_plus_21d"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "quantity",
        "last_restock_plus_21d",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.dense_rank().over(w_part).alias("quantity_dense_rank"),
        F.ntile(10).over(w_part_2).alias("quantity_bucket"),
        F.last("quantity", ignorenulls=True).over(w4).alias("last_known_quantity"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "quantity_dense_rank",
        "last_known_quantity",
        F.first("quantity_dense_rank", ignorenulls=True).over(w5).alias("first_known_quantity_dense_rank"),
        F.dense_rank().over(w6).alias("last_known_quantity_dense_rank"),
        F.percent_rank().over(w7).alias("last_known_quantity_percent_rank"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "last_known_quantity",
        "last_known_quantity_dense_rank",
        F.lead("last_known_quantity_dense_rank").over(w_recent).alias("next_last_known_quantity_dense_rank"),
        F.lag("last_known_quantity_dense_rank").over(w_recent).alias("prev_last_known_quantity_dense_rank"),
        F.lead("first_known_quantity_dense_rank").over(w_recent).alias("next_first_known_quantity_dense_rank"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.rank().over(w9).alias("last_known_quantity_dense_rank_rank"),
    )
)

with_windows_inventory = (
    with_windows
    .select(
        "id",
        "last_known_quantity_dense_rank",
        "next_first_known_quantity_dense_rank",
        "last_known_quantity_dense_rank_rank",
        F.sum("prev_last_known_quantity_dense_rank").over(w5).alias("running_sum_prev_last_known_quantity_dense_rank"),
        F.count("prev_last_known_quantity_dense_rank").over(w5).alias("running_count_prev_last_known_quantity_dense_rank"),
        F.avg("prev_last_known_quantity_dense_rank").over(w5).alias("running_avg_prev_last_known_quantity_dense_rank"),
        F.max("prev_last_known_quantity_dense_rank").over(w5).alias("running_max_prev_last_known_quantity_dense_rank"),
    )
)

filtered = (
    with_windows_inventory
    .filter(F.col("running_avg_prev_last_known_quantity_dense_rank") < 7.9)
    .select(
        "id",
        "last_known_quantity_dense_rank",
        "next_first_known_quantity_dense_rank",
        "last_known_quantity_dense_rank_rank",
        "running_sum_prev_last_known_quantity_dense_rank",
        "running_count_prev_last_known_quantity_dense_rank",
        F.when(F.col("last_known_quantity_dense_rank_rank") >= 7, "High").when(F.col("last_known_quantity_dense_rank_rank") >= 4, "Medium").otherwise("Low").alias("last_known_quantity_dense_rank_rank_band"),
        F.round((F.col("next_first_known_quantity_dense_rank") * 100), 2).alias("next_first_known_quantity_dense_rank_adjusted"),
    )
)

filtered_inventory = (
    filtered
    .filter(F.col("running_sum_prev_last_known_quantity_dense_rank") < 16)
    .withColumn("last_known_quantity_dense_rank_rank_plus_last_known_quantity_dense_rank", (F.col("last_known_quantity_dense_rank_rank") + F.col("last_known_quantity_dense_rank")))
)

result = (
    filtered_inventory
    .cube(
        F.when(F.col("running_count_prev_last_known_quantity_dense_rank") >= 14, "LARGE").otherwise("SMALL").alias("running_count_prev_last_known_quantity_dense_rank_tier"),
        F.when(F.col("last_known_quantity_dense_rank_rank") >= 8, "HIGH").otherwise("LOW").alias("last_known_quantity_dense_rank_rank_tier"),
        F.when(F.col("last_known_quantity_dense_rank_rank_plus_last_known_quantity_dense_rank") >= 74, "LARGE").otherwise("SMALL").alias("last_known_quantity_dense_rank_rank_plus_last_known_quantity_dense_rank_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
