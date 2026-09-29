from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("age")).otherwise(0)).alias("corporate_age"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
    .filter(F.col("corporate_age") > 69)
    .orderBy(F.col("corporate_age").desc())
)
