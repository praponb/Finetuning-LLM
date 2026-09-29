from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").asc_nulls_first(), "id")

with_dims = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.sqft"), F.col("b.city"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.product_name"), F.col("d.launch_date"), F.col("d.cost"))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("sqft_per_tokyo"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("chiang_mai_count") >= 1)
)
