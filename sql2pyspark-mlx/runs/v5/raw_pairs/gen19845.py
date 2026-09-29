from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

result = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.supplier_id",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)
