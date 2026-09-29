from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat("city", F.lit(" "), "country").alias("city_country"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", "s.city", F.col("c.brand"), F.col("c.category"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", "s.brand", "s.category", F.col("c.stock_qty"))
)

base_suppliers = (
    latest_match
    .filter(F.col("category").isNotNull())
    .select("*", F.round("stock_qty", 0).alias("stock_qty_rounded"))
)

result = (
    base_suppliers
    .select(
        "id",
        "supplier_name",
        "brand",
        "category",
        "stock_qty",
        "stock_qty_rounded",
        F.when(F.col("category").rlike("^(Sports|Grocery)$"), "EXTENDED").otherwise("PRIORITY").alias("category_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
