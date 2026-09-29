from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CHECKING"]) & (F.col("status") != "FROZEN") & (F.col("opened_date") <= "2025-05-06"))
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
    .orderBy(F.col("open_balance").desc())
)
