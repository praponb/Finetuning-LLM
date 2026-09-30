from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["TH", "SG", "JP"]))
    .select("name", "is_active", "credit_limit")
)
