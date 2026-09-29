from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id")

latest = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    latest
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.opened_date"), F.col("s.city"), F.col("s.region"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.brand"))
)

joined = (
    enriched
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.opened_date").alias("store_opened_date"),
        F.col("d.store_name"),
    )
)

result = (
    joined
    .select("id", "product_id", "units", "revenue", "city", "brand", "store_name")
    .orderBy("city")
)
