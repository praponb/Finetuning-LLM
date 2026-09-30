from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("revenue_per_unit", (F.col("revenue") * F.col("units")))
    .orderBy(F.col("sale_date").desc())
)
