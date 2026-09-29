from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "id",
        "opened_date",
        F.upper("status").alias("status_upper"),
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status"),
    )
    .orderBy(F.col("id").desc())
)
