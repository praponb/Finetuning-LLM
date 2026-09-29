from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w_ordered = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("unit_price") >= 210)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.discount"))
)

scored = (
    with_dims
    .select(
        "id",
        "quantity",
        "unit_price",
        F.lag("discount", 1).over(w).alias("prev_discount"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
        F.avg("quantity").over(w_ordered).alias("quantity_avg_5"),
    )
)

cleaned = (
    scored
    .filter(F.col("quantity_avg_5") <= 4)
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
    .withColumn("quantity_bucket_level", F.when(F.col("quantity_bucket") >= 9, "High").when(F.col("quantity_bucket") >= 2, "Medium").otherwise("Low"))
)

base = (
    cleaned
    .filter(F.col("quantity_bucket") == 5)
    .select(
        "id",
        "quantity",
        "unit_price",
        "prev_discount",
        "quantity_bucket",
        "quantity_avg_5",
        "greatest_quantity_unit_price",
        "quantity_bucket_level",
        (F.col("quantity_bucket") / F.nullif("prev_discount", F.lit(0))).alias("quantity_bucket_per_prev_discount"),
    )
)

scored_order_items = (
    base
    .filter(F.col("quantity_bucket") <= 5)
    .select(
        "*",
        F.first("greatest_quantity_unit_price", ignorenulls=True).over(w_running).alias("first_known_greatest_quantity_unit_price"),
        F.sum("unit_price").over(w5).alias("cumulative_unit_price"),
    )
)

result = (
    scored_order_items
    .select(
        "id",
        "unit_price",
        "first_known_greatest_quantity_unit_price",
        "greatest_quantity_unit_price",
    )
    .unpivot(
        ids=["id"],
        values=["unit_price", "first_known_greatest_quantity_unit_price", "greatest_quantity_unit_price"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
