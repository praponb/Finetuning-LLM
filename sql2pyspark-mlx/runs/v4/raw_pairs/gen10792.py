from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("price").alias("min_product_price"))

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

result = (
    with_latest
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("category").rlike("^(Books|Home)$"), "PRIORITY").when(F.col("category").rlike("^(Electronics|Sports)$"), "GROUP_A").otherwise("GROUP_B").alias("category_zone"),
        F.col("min_product_price"),
    )
    .orderBy("supplier_id", F.col("price").desc())
)
