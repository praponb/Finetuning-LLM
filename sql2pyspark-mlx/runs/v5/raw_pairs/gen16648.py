from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("category") == "Grocery"))
    .withColumn("cost_whole", F.floor("cost"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("DIRECT")))
)

eligible = (
    valid
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 5, 6])), "left_semi")
)

eligible_products = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 3, 5])), "left_semi")
)

with_last = (
    eligible_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

result = (
    with_last
    .filter(F.col("product_name").like("%Mak%"))
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "quantity",
        "unit_price",
        F.lag("unit_price").over(w2).alias("prev_unit_price"),
        F.lag("price").over(w2).alias("prev_price"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
