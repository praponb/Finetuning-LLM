from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Consumer", "Corporate"]) & (F.col("city") == "Bangkok"))
    .select("city")
)
