from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Initech")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.round((F.col("cost") * 100), 2).alias("cost_adjusted"),
    )
)

result = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.supplier_id",
        "s.cost_adjusted",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)
