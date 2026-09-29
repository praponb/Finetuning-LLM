from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("sqft_whole", F.ceil("sqft"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("NONE")))
)

result = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_anti")
)
