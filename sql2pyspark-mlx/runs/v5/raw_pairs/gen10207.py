from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "TH")
    .rollup("is_active", "segment")
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
)
