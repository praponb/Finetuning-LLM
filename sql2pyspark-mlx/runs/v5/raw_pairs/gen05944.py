from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", F.lit("customer").alias("record_type"))
    .union(
        df_shipments
        .select("shipped_date", F.lit("shipment").alias("record_type"))
    )
    .orderBy(F.col("record_type").desc())
)
