from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price_plus_line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price_rounded").desc_nulls_first())
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w4 = Window.orderBy("running_unit_price_rounded", "id")

valid = (
    df_order_items
    .filter(F.col("order_id").isin([1, 11, 17]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

filtered = (
    valid
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "unit_price_int",
        F.col("line_total").cast("int").alias("line_total_int"),
        (F.col("unit_price") + F.col("line_total")).alias("unit_price_plus_line_total"),
    )
)

cleaned = (
    filtered
    .filter(F.col("product_id").isNotNull())
    .select("*", F.round("unit_price", 1).alias("unit_price_rounded"))
)

scored = (
    cleaned
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total_int",
        "unit_price_plus_line_total",
        F.sum("unit_price_rounded").over(w).alias("running_unit_price_rounded"),
        F.dense_rank().over(w2).alias("unit_price_rounded_dense_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "product_id",
        "unit_price",
        "line_total_int",
        "running_unit_price_rounded",
        F.ntile(3).over(w3).alias("unit_price_bucket"),
        F.lag("unit_price_rounded_dense_rank", 2).over(w4).alias("prev_unit_price_rounded_dense_rank"),
    )
)
