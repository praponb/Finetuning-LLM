from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("unit_price_rounded", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("running_count_unit_price_rounded", "id")
w4 = Window.orderBy("unit_price_rounded_avg_3_times_unit_price_rounded", "id")

base = (
    df_order_items
    .filter(F.col("quantity").between(4, 7))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.round("unit_price", 1).alias("unit_price_rounded"),
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

metrics = (
    base
    .filter(F.col("unit_price_rounded").isNotNull())
    .select(
        "id",
        "quantity",
        "unit_price_rounded",
        "quantity_num",
        F.sum("unit_price_rounded").over(w).alias("running_sum_unit_price_rounded"),
        F.count("unit_price_rounded").over(w).alias("running_count_unit_price_rounded"),
        F.min("unit_price_rounded").over(w).alias("running_min_unit_price_rounded"),
        F.avg("unit_price_rounded").over(w2).alias("unit_price_rounded_avg_3"),
    )
)

filtered = (
    metrics
    .filter(F.col("unit_price_rounded_avg_3") <= 29)
    .withColumn("running_min_unit_price_rounded_rounded", F.round("running_min_unit_price_rounded", 1))
    .withColumn("unit_price_rounded_avg_3_times_unit_price_rounded", (F.col("unit_price_rounded_avg_3") * F.col("unit_price_rounded")))
)

enriched = (
    filtered
    .select(
        "id",
        "quantity",
        "unit_price_rounded",
        "running_sum_unit_price_rounded",
        "running_min_unit_price_rounded",
        "unit_price_rounded_avg_3",
        "running_min_unit_price_rounded_rounded",
        F.lead("quantity").over(w3).alias("next_quantity"),
        F.lag("quantity").over(w3).alias("prev_quantity"),
        F.lag("running_min_unit_price_rounded").over(w3).alias("prev_running_min_unit_price_rounded"),
        F.lead("running_min_unit_price_rounded").over(w3).alias("next_running_min_unit_price_rounded"),
        F.row_number().over(w3).alias("row_seq"),
        F.ntile(10).over(w4).alias("unit_price_rounded_avg_3_times_unit_price_rounded_bucket"),
    )
)

filtered_order_items = (
    enriched
    .filter(F.col("running_min_unit_price_rounded").between(45, 59))
    .select(
        "id",
        "quantity",
        "running_sum_unit_price_rounded",
        "unit_price_rounded_avg_3",
        "running_min_unit_price_rounded_rounded",
        "next_quantity",
        "prev_running_min_unit_price_rounded",
        "next_running_min_unit_price_rounded",
        "row_seq",
        F.least("unit_price_rounded", "prev_running_min_unit_price_rounded").alias("least_unit_price_rounded_prev_running_min_unit_price_rounded"),
    )
)

result = (
    filtered_order_items
    .select(
        "id",
        "quantity",
        "running_sum_unit_price_rounded",
        "unit_price_rounded_avg_3",
        "running_min_unit_price_rounded_rounded",
        "next_quantity",
        "prev_running_min_unit_price_rounded",
        "next_running_min_unit_price_rounded",
        "row_seq",
    )
    .orderBy(F.col("next_running_min_unit_price_rounded").desc_nulls_last())
)
