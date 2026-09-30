from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-04-01", "2024-07-18"))
    .select("status")
    .distinct()
)
