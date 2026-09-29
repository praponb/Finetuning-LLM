from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2023-11-15") & (F.col("account_type") == "CHECKING"))
    .select(
        "status",
        "opened_date",
        "account_type",
        F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
        F.lower("status").alias("status_lower"),
    )
)
