from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2023-06-01")
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isin([14, 23, 25]))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
