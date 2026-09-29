from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 2, 7]) & (F.col("segment") == "Corporate"))
    .groupBy("segment")
    .agg(F.count("*").alias("num_customers"))
    .orderBy("segment")
    .limit(50)
)
