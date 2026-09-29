from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("product_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (~F.col("ch.brand").isin(["Acme", "Globex"])), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"), F.col("c.price").alias("product_price"))
)

enriched = (
    with_latest
    .withColumn("cumulative_product_price", F.sum("product_price").over(w2))
    .withColumn("country_max_stock_qty", F.max("stock_qty").over(w3))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w4))
)

result = (
    enriched
    .select(
        "id",
        "city",
        "product_price",
        "cumulative_product_price",
        "country_max_stock_qty",
    )
)
