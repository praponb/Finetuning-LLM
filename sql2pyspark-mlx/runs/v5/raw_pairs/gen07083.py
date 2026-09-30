from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.lit("shipment").alias("kind"), "delivered_date")
    .union(df_customers.select(F.lit("customer").alias("kind"), "signup_date"))
)
