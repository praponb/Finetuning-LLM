from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-09-25", "2025-03-01"))
    .select("end_date")
    .intersect(df_accounts.select("opened_date"))
    .orderBy("end_date")
)
