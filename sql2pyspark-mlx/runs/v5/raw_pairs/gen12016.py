from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CREDIT", "CHECKING"]))
    .select(
        "id",
        "status",
        F.upper("status").alias("status_upper"),
        F.when(F.col("account_type") == "CHECKING", 1).otherwise(0).alias("is_checking"),
    )
    .orderBy("status", "status_upper")
)
