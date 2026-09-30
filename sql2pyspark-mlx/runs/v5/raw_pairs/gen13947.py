from pyspark.sql import functions as F

base = df_customers.filter(F.col("segment").isin(["SMB", "Consumer", "Corporate"]))

customers_stats = (
    base
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0))) / F.count("*")), 1).alias("pct_corporate"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

result = customers_stats.orderBy(F.col("pct_corporate").desc())
