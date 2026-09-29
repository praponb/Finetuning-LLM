from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB"]) & F.col("segment").isin(["Consumer", "Corporate"]))
    .select("id", "signup_date", "country", "city")
)
