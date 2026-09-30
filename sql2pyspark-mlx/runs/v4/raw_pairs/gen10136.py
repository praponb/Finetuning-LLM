from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2025-03-01") & ((F.col("city") == "London") | F.col("city").isin(["Berlin", "Singapore"])))
    .select("id", "sqft")
)
