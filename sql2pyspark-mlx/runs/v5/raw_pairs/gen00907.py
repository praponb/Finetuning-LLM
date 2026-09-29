from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.rating",
        F.col("c.category"),
        F.col("c.stock_qty"),
    )
)

result = (
    latest_match
    .select("id", "supplier_name", "country", "rating", "category", "stock_qty")
    .orderBy(F.col("country").asc_nulls_last())
)
