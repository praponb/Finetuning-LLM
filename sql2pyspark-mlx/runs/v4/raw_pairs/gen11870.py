from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CHECKING", "CREDIT"]))
    .select(F.col("opened_date").alias("event_date"), F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "standard")
        .select(
        F.col("start_date").alias("event_date"),
        F.lit("subscription").alias("source"),
    )
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
