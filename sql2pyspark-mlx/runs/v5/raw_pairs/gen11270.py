from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") == "UK"))
    .withColumn("rating_rounded", F.round("rating", 2))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

with_last = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"), F.col("c.cost").alias("product_cost"))
)

kept = (
    with_last
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category").isin(["Fashion", "Books", "Home"])), "left_anti")
)

with_last_suppliers = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.rating", "s.city_clean", "s.price", F.col("c.launch_date"))
)

result = (
    with_last_suppliers
    .select("id", "rating", "city_clean", "price", "launch_date")
    .orderBy(F.col("launch_date").desc())
)
