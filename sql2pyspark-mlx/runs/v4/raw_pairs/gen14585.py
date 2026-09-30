from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

enriched = (
    base
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.category"))
)

enriched_order_items = (
    enriched
    .select(
        "*",
        F.sum("line_total").over(w).alias("running_line_total"),
        F.avg("quantity").over(w2).alias("quantity_avg_5"),
    )
)

result = (
    enriched_order_items
    .select(
        "id",
        "order_id",
        F.col("line_total").alias("LINE_TOTAL_VAL"),
        F.col("unit_price").alias("UNIT_PRICE"),
    )
    .unpivot(["id", "order_id"], ["LINE_TOTAL_VAL", "UNIT_PRICE"], "kpi", "value")
    .filter(F.col("value").isNotNull())
)
