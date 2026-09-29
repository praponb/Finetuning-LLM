from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "UK", 1).otherwise(0))) / F.count("*")), 1).alias("pct_uk"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
)
