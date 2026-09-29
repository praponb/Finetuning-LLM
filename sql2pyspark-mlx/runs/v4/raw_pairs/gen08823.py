from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("line_total").desc_nulls_first())
w3 = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

filtered = (
    valid
    .filter(F.col("line_total") > 66)
    .withColumn("line_total_level", F.when(F.col("line_total") >= 66, "High").when(F.col("line_total") >= 30, "Medium").otherwise("Low"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.percent_rank().over(w2).alias("line_total_percent_rank"),
        F.sum("line_total").over(w3).alias("running_line_total"),
        F.sum("unit_price").over(w4).alias("running_unit_price"),
    )
)

result = (
    with_windows
    .select("id", "order_id", "unit_price", F.col("line_total").alias("LINE_TOTAL_VAL"))
    .unpivot(["id", "order_id"], ["unit_price", "LINE_TOTAL_VAL"], "metric", "val")
)
