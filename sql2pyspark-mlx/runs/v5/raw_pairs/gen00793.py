from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]))
    .select("status")
    .intersect(df_subscriptions.filter(F.col("status") == "EXPIRED").select("status"))
    .orderBy("status")
)
