from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("units_whole", F.floor("units"))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
    .orderBy(F.col("store_id").desc())
)
