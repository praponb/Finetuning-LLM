from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy(F.col("greatest_unit_price_quantity").desc_nulls_last())

filtered = (
    df_order_items
    .filter(F.col("quantity") > 7)
    .withColumn("greatest_unit_price_quantity", F.greatest("unit_price", "quantity"))
)

metrics = (
    filtered
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.first("unit_price", ignorenulls=True).over(w_ordered).alias("first_known_unit_price"),
        F.cume_dist().over(w_recent).alias("greatest_unit_price_quantity_cume_dist"),
        F.sum("greatest_unit_price_quantity").over(w_ordered).alias("cumulative_greatest_unit_price_quantity"),
    )
)

valid = (
    metrics
    .filter(F.col("product_id").isNotNull())
    .withColumn("first_known_unit_price_filled", F.coalesce("first_known_unit_price", F.lit(0)))
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.channel"))
)

valid_order_items = (
    joined
    .filter(F.col("product_id").isin([6, 15, 26, 36]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "first_known_unit_price",
        "cumulative_greatest_unit_price_quantity",
        (F.col("first_known_unit_price_filled") - F.col("greatest_unit_price_quantity_cume_dist")).alias("first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist"),
    )
)

filtered_order_items = (
    valid_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "first_known_unit_price",
        "cumulative_greatest_unit_price_quantity",
        "first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist",
        (F.col("unit_price") * F.col("first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist")).alias("unit_price_times_first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist"),
        (F.col("unit_price") + F.col("first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist")).alias("unit_price_plus_first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist"),
    )
)

result = (
    filtered_order_items
    .select(
        "id",
        "order_id",
        "first_known_unit_price",
        "cumulative_greatest_unit_price_quantity",
        "first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist",
        "unit_price_plus_first_known_unit_price_filled_minus_greatest_unit_price_quantity_cume_dist",
    )
)
