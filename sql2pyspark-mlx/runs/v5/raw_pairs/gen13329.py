from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Berlin")
    .select("opened_date")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") < "2024-04-02")
        .select(F.col("end_date").alias("opened_date"))
    )
)
