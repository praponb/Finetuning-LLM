from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5]) & (F.col("units").isNotNull() | F.col("sale_date").between("2024-04-01", "2025-05-01")))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
)

store_id_summary = (
    src
    .groupBy("store_id")
    .agg(F.max("units").alias("max_units"))
)

result = (
    store_id_summary
    .filter(F.col("max_units") < 19)
    .select("store_id", "max_units")
)
