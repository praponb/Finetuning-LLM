from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .intersect(
        df_subscriptions
        .filter(F.month("start_date").isin([2, 7, 8]))
        .select("end_date")
    )
    .orderBy(F.col("shipped_date").desc())
)
