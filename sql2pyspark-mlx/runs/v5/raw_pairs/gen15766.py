from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy("cost", "id")

with_dims = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.units",
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.sqft",
        "e.opened_date",
        F.col("d.brand"),
        F.col("d.category"),
        F.col("d.cost"),
    )
)

ranked_store_sales = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_store_sales
    .select("id", "product_id", "sqft", "opened_date", "brand", "cost", "latest_rank")
    .orderBy(F.col("product_id").desc())
)
