from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.supplier_id",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    with_latest
    .select("id", "product_name", "cost", "launch_date", "inventory_quantity")
    .orderBy(F.col("inventory_quantity").asc_nulls_first(), F.col("cost").desc())
)
