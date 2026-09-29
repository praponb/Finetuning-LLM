from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNotNull() & ~F.col("email").like("%@yahoo.com"))
    .select("segment", F.col("is_active").cast("int").alias("is_active_flag"))
    .orderBy("is_active_flag", "segment")
)
