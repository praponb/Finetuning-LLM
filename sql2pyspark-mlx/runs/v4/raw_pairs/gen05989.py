from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2023-09-11")
    .select("plan", "start_date")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date").between("2025-04-01", "2025-08-21"))
        .select("plan", "start_date")
    )
    .orderBy("plan")
)
