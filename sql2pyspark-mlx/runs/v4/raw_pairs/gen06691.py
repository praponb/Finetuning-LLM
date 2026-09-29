from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"), F.col("c.units"))
)

result = (
    latest_match
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "store_sale_revenue",
    )
    .orderBy(F.col("id").desc())
)
