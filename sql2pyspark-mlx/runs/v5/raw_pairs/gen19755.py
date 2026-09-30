from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "SG")
    .groupBy("segment", "country")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("credit_limit")).otherwise(0)).alias("singapore_credit_limit"),
        F.sum("age").alias("total_age"),
        F.sum(F.when(F.col("country") == "SG", F.col("credit_limit")).otherwise(0)).alias("sg_credit_limit"),
    )
    .orderBy(F.col("segment").desc(), "country")
)
