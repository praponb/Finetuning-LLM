from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2025-04-23", "2025-06-01"))
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["CANCELLED", "ACTIVE", "EXPIRED"]))
        .select("status")
    )
    .distinct()
)
