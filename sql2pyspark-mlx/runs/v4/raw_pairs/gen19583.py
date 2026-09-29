from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([7, 9, 11]))
    .select("status")
    .subtract(df_subscriptions.filter(F.col("monthly_fee") <= 220).select("status"))
)
