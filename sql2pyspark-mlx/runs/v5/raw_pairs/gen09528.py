from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("line_total").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id") == 23)
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    latest
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.order_date").between("2023-03-13", "2024-06-01"))
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.unit_price",
        "a.greatest_quantity_unit_price",
        F.col("b.channel"),
    )
)

with_dims_order_items = (
    with_dims
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        "src.greatest_quantity_unit_price",
        F.col("p.discount"),
        F.col("p.channel").alias("order_channel"),
        F.col("p.order_date"),
    )
)

base = (
    with_dims_order_items
    .filter(F.col("order_channel").isNotNull() & F.col("order_id").isin([22, 28]))
    .select(
        "id",
        "order_id",
        "product_id",
        "greatest_quantity_unit_price",
        "discount",
        "order_date",
        F.when(F.col("unit_price") >= 360, "High").when(F.col("unit_price") >= 90, "Medium").otherwise("Low").alias("unit_price_tier"),
        F.coalesce("order_channel", F.lit("N/A")).alias("order_channel_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "product_id",
        "greatest_quantity_unit_price",
        "discount",
        "order_date",
        "unit_price_tier",
    )
    .orderBy(F.col("greatest_quantity_unit_price").desc_nulls_last())
)
