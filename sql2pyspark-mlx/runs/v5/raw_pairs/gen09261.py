from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([2, 10, 11]))
    .select(
        "id",
        "status",
        "end_date",
        F.when(F.col("status").isin(["ACTIVE", "EXPIRED"]), "Priority").otherwise("Other").alias("status_group"),
    )
)
