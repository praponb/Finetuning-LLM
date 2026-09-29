from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"))
    .filter(F.col("bangkok_count") < 3)
)
