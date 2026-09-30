from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .union(df_shipments.select("shipped_date"))
    .distinct()
    .orderBy(F.col("signup_date").desc())
)
