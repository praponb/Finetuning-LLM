from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2025-02-02", "2025-09-07"))
    .select("status")
    .unionByName(df_subscriptions.filter(F.col("monthly_fee") > 180).select("status"))
)
