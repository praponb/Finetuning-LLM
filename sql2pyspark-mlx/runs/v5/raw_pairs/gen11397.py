from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") != "Corporate")
    .groupBy("is_active")
    .agg(F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"))
    .filter(F.col("corporate_count") < 4)
    .select("is_active", "corporate_count")
)
