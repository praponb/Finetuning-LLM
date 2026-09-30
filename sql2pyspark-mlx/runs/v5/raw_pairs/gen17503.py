from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("age").alias("age_count"),
    )
    .orderBy("country")
)
