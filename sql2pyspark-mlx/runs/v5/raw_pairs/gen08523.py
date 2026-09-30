from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Flash"]))
    .select("shipped_date")
    .intersect(
        df_subscriptions
        .filter(F.month("end_date").isin([2, 4, 12]))
        .select("end_date")
    )
)
