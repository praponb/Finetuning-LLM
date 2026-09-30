from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "category")
w2 = Window.partitionBy("city", "category").orderBy(F.col("revenue").desc())

base = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

joined = (
    base
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.opened_date"), F.col("d.sqft"))
)

enriched = (
    joined
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.category"), F.col("p.product_name"), F.col("p.launch_date"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.city"), F.col("s.opened_date").alias("store_opened_date"))
)

metrics = (
    with_dims
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w)), 4))
    .withColumn("revenue_dense_rank", F.dense_rank().over(w2))
)

result = (
    metrics
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 14748, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy(F.col("sqft_tier").desc())
)
