from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "SMB"]) & (F.upper(F.trim("email")) == "EMMA@OUTLOOK.COM"))
)

summary = (
    recent_customers
    .groupBy("segment")
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.min("age").alias("min_age"),
    )
)

result = (
    summary
    .select("segment", "credit_limit_count", "min_age")
    .orderBy("segment", F.col("credit_limit_count").desc())
)
