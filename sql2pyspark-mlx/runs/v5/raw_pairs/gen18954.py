from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w3 = Window.partitionBy("account_type").orderBy("balance_whole", "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-07-08").cast("date")) & (~F.col("status").eqNullSafe(F.lit("OPEN"))))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned_accounts = (
    latest
    .filter(F.col("status").eqNullSafe(F.lit("CLOSED")))
    .select(
        "*",
        F.date_add("opened_date", 81).alias("opened_date_plus_81d"),
        F.ceil("balance").alias("balance_whole"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

deduped = (
    cleaned_accounts
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "account_type",
        "opened_date",
        "days_since_opened_date",
        "status_filled",
        "opened_date_plus_81d",
        "status_clean",
    )
)

result = (
    ranked
    .select(
        "id",
        "account_type",
        "opened_date",
        "status_filled",
        "opened_date_plus_81d",
        "status_clean",
    )
    .orderBy("opened_date_plus_81d")
)
