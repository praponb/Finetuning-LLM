from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("line_total_plus_quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("line_total_plus_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("line_total_plus_quantity", "id")
w4 = Window.partitionBy("product_id").orderBy("row_seq", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("unit_price_per_line_total_plus_quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("least_quantity_row_seq").asc_nulls_last())

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("quantity") * F.col("unit_price")).alias("line_total"))
)

base_order_items = (
    base
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("line_total") + F.col("quantity")).alias("line_total_plus_quantity"),
    )
)

with_windows = (
    base_order_items
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w2))
    .withColumn("line_total_plus_quantity_bucket", F.ntile(3).over(w3))
)

base_order_items_2 = (
    with_windows
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "row_seq",
        "cumulative_quantity",
        F.least("quantity", "row_seq").alias("least_quantity_row_seq"),
        F.round((F.col("unit_price") / F.col("line_total_plus_quantity_bucket")), 2).alias("unit_price_per_line_total_plus_quantity_bucket"),
    )
)

result = (
    base_order_items_2
    .select(
        "*",
        F.avg("row_seq").over(w4).alias("row_seq_avg_6"),
        F.sum("cumulative_quantity").over(w5).alias("cumulative_cumulative_quantity"),
        F.percent_rank().over(w6).alias("least_quantity_row_seq_percent_rank"),
    )
)
