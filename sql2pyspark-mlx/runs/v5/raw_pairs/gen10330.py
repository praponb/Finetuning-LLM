from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & ((F.col("age") >= 54) | (F.col("segment") == "SMB")))
    .groupBy("country", "city")
    .agg(F.max("credit_limit").alias("max_credit_limit"))
    .orderBy("country")
)
