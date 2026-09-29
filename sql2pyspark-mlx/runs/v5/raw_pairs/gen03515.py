from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .select("start_date")
    .intersect(
        df_subscriptions
        .filter(F.col("plan") != "standard")
        .select("start_date")
    )
)
