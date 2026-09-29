from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.col("amount").alias("value"))
    .intersect(
        df_shipments
        .filter(F.col("delivered_date") < "2025-09-28")
        .select(F.col("cost").alias("value"))
    )
)
