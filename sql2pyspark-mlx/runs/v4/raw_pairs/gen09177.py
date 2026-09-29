from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(4700, 13700) & (F.col("credit_limit") <= 13200) & ~F.col("is_active"))
    .groupBy("city")
    .agg(F.countDistinct("credit_limit").alias("unique_segments"))
    .orderBy("city")
)
