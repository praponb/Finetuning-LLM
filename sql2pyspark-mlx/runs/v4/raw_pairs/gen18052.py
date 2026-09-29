from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") != "New York") & (F.col("city") == "Bangkok") & (F.col("credit_limit") >= 9000))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("credit_limit_whole", F.ceil("credit_limit"))
)
