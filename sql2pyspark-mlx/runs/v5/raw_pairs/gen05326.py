from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(10500, 11700) & (F.col("segment") != "Consumer") & F.col("name").isNotNull())
    .groupBy("is_active")
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
)
