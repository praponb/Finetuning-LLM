from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-03"))) & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

result = (
    filtered
    .select("customer_id", "account_type")
    .intersectAll(
        df_accounts
        .filter(F.col("status") == "OPEN")
        .select("customer_id", "account_type")
    )
)
