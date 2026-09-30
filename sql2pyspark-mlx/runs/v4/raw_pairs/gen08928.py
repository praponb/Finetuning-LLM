from pyspark.sql import functions as F

valid_customers = df_customers.filter(~((F.col("city") == "New York") | F.col("is_active")))

summary = (
    valid_customers
    .groupBy("city")
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

result = summary
