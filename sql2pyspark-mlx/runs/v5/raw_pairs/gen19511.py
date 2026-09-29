from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-06-01", "2024-06-01"))
    .select("status", "plan")
    .distinct()
    .orderBy("plan", "status")
    .limit(5)
)
