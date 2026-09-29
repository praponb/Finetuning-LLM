from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3900)
    .withColumn("revenue_per_unit", (F.col("revenue") + F.col("units")))
    .withColumn("units_scaled", F.round((F.col("units") * 1.1), 2))
    .orderBy(F.col("units_scaled").desc(), "id")
    .limit(50)
)
