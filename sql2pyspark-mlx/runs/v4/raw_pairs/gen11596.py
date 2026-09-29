from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2023-10-01")
    .select("start_date", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.month("end_date").isin([6, 7, 8]))
        .select("start_date", "end_date")
    )
    .distinct()
)
