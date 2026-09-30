from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "UK") & (~F.col("city").isin(["Singapore", "Bangkok"]) | F.col("is_active")))
    .select("id", "segment", "credit_limit")
)
