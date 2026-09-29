from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sqft").desc_nulls_last())
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock_month_end", "id")

filtered = (
    df_inventory
    .filter(F.col("quantity").between(190, 230))
    .select(
        "id",
        "store_id",
        "product_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.sqft"), F.col("p2.region"))
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "last_restock_month_end", "sqft", "region")
)

enriched = (
    ranked
    .withColumn("sqft_bucket", F.ntile(10).over(w2))
    .withColumn("sqft_bucket_2", F.ntile(4).over(w2))
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w3))
)

result = (
    enriched
    .select("id", "sqft", "region", "sqft_bucket_2", "prev_sqft")
    .orderBy(F.col("sqft").desc())
)
