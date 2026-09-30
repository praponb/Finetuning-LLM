from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") <= "2025-09-01")
        .select(F.col("end_date").alias("opened_date"))
    )
    .distinct()
    .orderBy("opened_date")
)
