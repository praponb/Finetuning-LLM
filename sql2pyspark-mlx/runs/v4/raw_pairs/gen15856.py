from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .intersect(
        df_subscriptions
        .filter(F.col("monthly_fee") <= 220)
        .select("monthly_fee")
    )
    .orderBy("cost")
)
