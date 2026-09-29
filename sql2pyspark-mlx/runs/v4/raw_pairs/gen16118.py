from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isin([14, 38]))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
)

base = (
    cleaned
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.round((F.col("quantity_str") / F.col("quantity")), 2).alias("quantity_str_per_quantity"),
        F.round((F.col("quantity_str") / F.col("quantity")), 2).alias("quantity_str_per_quantity_2"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.cost"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.amount"), F.col("o.order_date"))
)

with_windows = (
    with_dims
    .withColumn("last_known_quantity_str_per_quantity_2", F.last("quantity_str_per_quantity_2", ignorenulls=True).over(w))
    .withColumn("running_unit_price", F.sum("unit_price").over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "quantity_str",
        "quantity_str_per_quantity",
        "quantity_str_per_quantity_2",
        "cost",
        "amount",
    )
    .orderBy(F.col("quantity_str_per_quantity_2").asc_nulls_first())
)
