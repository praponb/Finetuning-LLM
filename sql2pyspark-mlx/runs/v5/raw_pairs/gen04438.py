from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]) & F.col("account_type").isin(["CHECKING", "LOAN"]))
    .select(
        "id",
        "opened_date",
        F.concat("account_type", F.lit(" - "), "status").alias("account_type_status"),
        F.date_sub("opened_date", 14).alias("opened_date_minus_14d"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
