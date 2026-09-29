from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-06-10").cast("date")) & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .select(
        "*",
        F.ceil("balance").alias("balance_whole"),
        F.col("balance").cast("int").alias("balance_int"),
    )
)

result = (
    valid
    .select("id", "status")
    .subtract(
        df_accounts
        .filter(F.col("status") == "OPEN")
        .select("id", "status")
    )
)
