from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.rating",
        "s.rating_filled",
        "s.supplier_name_title",
        F.col("c.cost"),
        F.col("c.price").alias("product_price"),
    )
)
