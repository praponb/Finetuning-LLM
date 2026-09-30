from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
)
