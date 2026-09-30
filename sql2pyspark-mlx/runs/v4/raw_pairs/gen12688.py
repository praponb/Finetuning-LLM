from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") != "Consumer")
    .select("id", "name", F.round("credit_limit", 2).alias("credit_limit_rounded"))
)
