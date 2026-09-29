from pyspark.sql import functions as F

base = df_customers.filter(F.col("email").isNotNull())

agg = (
    base
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
)

result = agg
