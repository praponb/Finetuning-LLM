from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("name").like("Al%") & F.col("country").isin(["DE", "US"])) | (F.col("segment") == "Corporate"))
    .agg(
        F.sum("age").alias("total_age"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
)
