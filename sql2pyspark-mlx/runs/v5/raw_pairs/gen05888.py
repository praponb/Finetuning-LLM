from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.tags",
        F.col("c.units"),
    )
)

result = (
    latest_match
    .select("id", "product_name", "brand", "units")
    .orderBy(F.col("id").desc(), F.col("brand").desc())
    .limit(20)
)
