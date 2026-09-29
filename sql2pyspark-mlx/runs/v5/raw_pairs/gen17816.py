from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("unit_price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("unit_price", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("order_id")
w5 = Window.orderBy("cumulative_quantity_minus_quantity", "id")
w6 = Window.orderBy("cumulative_quantity", "id")

valid = (
    df_order_items
    .filter(F.col("unit_price") >= 210)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

scored = (
    valid
    .select(
        "id",
        "quantity",
        "unit_price",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
        F.avg("quantity").over(w3).alias("quantity_avg_3_prior"),
        F.corr("quantity", "unit_price").over(w4).alias("corr_quantity_unit_price"),
        F.count("unit_price").over(w4).alias("order_id_count_unit_price"),
    )
)

base = (
    scored
    .filter(F.col("order_id_count_unit_price") < 10)
    .select(
        "*",
        (F.col("quantity") - F.col("cumulative_quantity")).alias("quantity_minus_cumulative_quantity"),
        (F.col("cumulative_quantity") - F.col("quantity")).alias("cumulative_quantity_minus_quantity"),
    )
)

with_windows = (
    base
    .withColumn("cumulative_quantity_minus_quantity_bucket", F.ntile(4).over(w5))
    .withColumn("prev_unit_price", F.lag("unit_price", 2).over(w6))
)

result = (
    with_windows
    .select(
        "id",
        F.col("unit_price").alias("UNIT_PRICE"),
        F.col("corr_quantity_unit_price").alias("CORR_QUANTITY_UNIT_PRICE"),
        "quantity_minus_cumulative_quantity",
    )
    .unpivot(
        ids=["id"],
        values=["UNIT_PRICE", "CORR_QUANTITY_UNIT_PRICE", "quantity_minus_cumulative_quantity"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
