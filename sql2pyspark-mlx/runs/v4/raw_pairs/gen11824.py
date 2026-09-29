from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .select("signup_date", F.lit("customer").alias("source"))
    .union(
        df_shipments
        .filter(F.col("cost") <= 83)
        .select("delivered_date", F.lit("shipment").alias("source"))
    )
    .orderBy("signup_date")
)
