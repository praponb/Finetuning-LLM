from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select("status")
    .subtract(df_subscriptions.filter(F.col("plan") != "premium").select("status"))
)
