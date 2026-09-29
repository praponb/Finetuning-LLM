from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
    .select("signup_date")
    .union(df_shipments.select("shipped_date"))
    .distinct()
    .orderBy("signup_date")
)
