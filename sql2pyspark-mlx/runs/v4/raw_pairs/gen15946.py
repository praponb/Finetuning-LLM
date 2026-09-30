from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("plan", "id")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") >= "2023-12-01")
        .select("plan", "id")
    )
)
