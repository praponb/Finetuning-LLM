from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-06-30").cast("date"))
    .withColumn("region_filled", F.coalesce("region", F.lit("unassigned")))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

result = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id").isin([7, 8])), "left_anti")
)
