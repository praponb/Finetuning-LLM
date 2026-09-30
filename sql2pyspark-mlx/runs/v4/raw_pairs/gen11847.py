from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("quantity_rounded", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 220))
    .withColumn("quantity_rounded", F.round("quantity", 1))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

metrics = (
    filtered
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w))
    .withColumn("quantity_rounded_bucket", F.ntile(3).over(w2))
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .select(
        "id",
        "product_id",
        F.col("line_total").alias("LINE_TOTAL_VAL"),
        F.col("unit_price").alias("UNIT_PRICE"),
        "quantity_rounded",
    )
    .unpivot(
        ["id", "product_id"],
        ["LINE_TOTAL_VAL", "UNIT_PRICE", "quantity_rounded"],
        "metric",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
