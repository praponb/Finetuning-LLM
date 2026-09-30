from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.lit("customer").alias("source"), "signup_date")
    .union(df_shipments.select(F.lit("shipment").alias("source"), "shipped_date"))
)
