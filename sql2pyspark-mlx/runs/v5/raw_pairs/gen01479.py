from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "JP"]))
    .select("id", "segment", "city", "credit_limit")
)
