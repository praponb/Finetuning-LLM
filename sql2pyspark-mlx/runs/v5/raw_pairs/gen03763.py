from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city") == "Bangkok"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.supplier_name_title",
        F.col("c.brand"),
        F.col("c.stock_qty"),
    )
)
