from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("least_last4_avg_quantity_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("running_least_last4_avg_quantity_quantity", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("last4_sum_quantity", "id").rowsBetween(-6, -1)

cleaned = (
    df_inventory
    .filter(F.col("product_id").isin([4, 6, 7]))
    .select(
        "id",
        "store_id",
        "quantity",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "quantity",
        F.avg("quantity").over(w_hist).alias("last4_avg_quantity"),
        F.sum("quantity").over(w_hist).alias("last4_sum_quantity"),
        F.count("quantity").over(w_hist).alias("last4_count_quantity"),
    )
)

valid = (
    scored
    .filter(F.col("last4_sum_quantity").isNotNull())
    .select(
        "id",
        "quantity",
        "last4_avg_quantity",
        "last4_sum_quantity",
        "last4_count_quantity",
        F.least("last4_avg_quantity", "quantity").alias("least_last4_avg_quantity_quantity"),
    )
)

with_windows = (
    valid
    .filter(F.col("quantity") >= 113)
    .select(
        "*",
        F.last("least_last4_avg_quantity_quantity", ignorenulls=True).over(w2).alias("last_known_least_last4_avg_quantity_quantity"),
        F.sum("least_last4_avg_quantity_quantity").over(w3).alias("running_least_last4_avg_quantity_quantity"),
    )
)

with_windows_inventory = (
    with_windows
    .select(
        "*",
        F.sum("running_least_last4_avg_quantity_quantity").over(w_recent).alias("last3_sum_running_least_last4_avg_quantity_quantity"),
        F.count("running_least_last4_avg_quantity_quantity").over(w_recent).alias("last3_count_running_least_last4_avg_quantity_quantity"),
        F.min("running_least_last4_avg_quantity_quantity").over(w_recent).alias("last3_min_running_least_last4_avg_quantity_quantity"),
        F.avg("last4_avg_quantity").over(w5).alias("last4_avg_quantity_avg_6_prior"),
    )
)

result = (
    with_windows_inventory
    .select(
        "id",
        "quantity",
        "last4_avg_quantity",
        "last4_sum_quantity",
        "last4_count_quantity",
        "least_last4_avg_quantity_quantity",
        "last_known_least_last4_avg_quantity_quantity",
        "running_least_last4_avg_quantity_quantity",
        "last3_sum_running_least_last4_avg_quantity_quantity",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
