from pyspark.sql import functions as F

src = (
    df_accounts
    .filter(F.month("opened_date").isin([6, 11, 12]) | F.col("account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]))
)

agg = (
    src
    .groupBy("account_type")
    .agg(F.sum("balance").alias("total_balance"))
)

result = (
    agg
    .filter(F.col("total_balance") < 142000)
    .orderBy(F.col("total_balance").desc())
)
