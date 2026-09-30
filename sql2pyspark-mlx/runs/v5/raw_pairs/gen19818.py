from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-09"))) & F.col("status").isNotNull())
    .select(
        "*",
        F.year("opened_date").alias("opened_date_year"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

result = (
    cleaned
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("customer_id") == 36)
        .select("id")
    )
)
