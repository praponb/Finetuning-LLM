from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price_rounded").desc(), "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("unit_price") <= 240)
    .select(
        "id",
        "quantity",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

base = (
    valid
    .filter(F.col("quantity") <= 6)
    .select(
        "*",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        (F.col("unit_price") / F.nullif("least_unit_price_quantity", F.lit(0))).alias("unit_price_per_least_unit_price_quantity"),
    )
)

valid_order_items = (
    base
    .filter(F.col("least_unit_price_quantity").between(28, 63))
    .select(
        "id",
        "quantity",
        "unit_price",
        "least_unit_price_quantity",
        "quantity_filled",
        (F.col("unit_price_per_least_unit_price_quantity") - F.col("unit_price")).alias("unit_price_per_least_unit_price_quantity_minus_unit_price"),
        F.round("unit_price", 0).alias("unit_price_rounded"),
    )
)

metrics = (
    valid_order_items
    .select(
        "*",
        F.ntile(5).over(w).alias("unit_price_rounded_bucket"),
        F.last("unit_price", ignorenulls=True).over(w2).alias("last_known_unit_price"),
    )
)

result = (
    metrics
    .select(
        "id",
        "unit_price",
        "least_unit_price_quantity",
        "quantity_filled",
        "unit_price_rounded",
        "last_known_unit_price",
    )
    .orderBy("unit_price", F.col("unit_price_rounded").desc())
)
