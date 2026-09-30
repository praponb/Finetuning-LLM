from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS"]))
    .select("opened_date")
    .subtract(df_subscriptions.select("start_date"))
    .orderBy("opened_date")
)
