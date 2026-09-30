from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "CREDIT"]) & (F.col("opened_date") >= F.lit("2023-06-12").cast("date")))
    .select(
        "*",
        F.upper("status").alias("status_upper"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

result = (
    filtered
    .select("id", "status")
    .intersectAll(
        df_accounts
        .filter(F.col("status").isNotNull())
        .select("id", "status")
    )
)
