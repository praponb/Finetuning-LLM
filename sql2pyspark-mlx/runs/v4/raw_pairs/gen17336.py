from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED"]))
    .select("end_date")
    .subtract(df_subscriptions.filter(F.col("monthly_fee") < 180).select("end_date"))
)
