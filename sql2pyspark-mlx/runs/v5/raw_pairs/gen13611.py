from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select("shipped_date")
    .intersect(
        df_subscriptions
        .filter(F.year("start_date") >= 2023)
        .select("end_date")
    )
    .orderBy("shipped_date")
)
