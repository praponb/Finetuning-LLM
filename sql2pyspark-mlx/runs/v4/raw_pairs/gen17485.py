from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & F.col("credit_limit").between(11300, 12200) & F.col("is_active"))
    .groupBy("city")
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
    .filter(F.col("stddev_age") < 31)
)
