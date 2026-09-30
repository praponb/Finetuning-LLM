from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "EXPIRED")
    .select("start_date", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") > "2023-05-01")
        .select("start_date", "status")
    )
)
