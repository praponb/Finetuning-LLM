from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-05-26"))) & F.col("supplier_id").isNotNull())
    .withColumn("cost_whole", F.ceil("cost"))
)

result = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        F.col("c.unit_price"),
    )
)
