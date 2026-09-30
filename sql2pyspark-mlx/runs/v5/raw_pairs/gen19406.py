from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2024-03-04")
    .select(
        "id",
        "started_at",
        "customer_id",
        "country",
        F.when(F.col("country").isin(["JP", "DE"]), "Group A").otherwise("Other").alias("country_group"),
    )
    .orderBy(F.col("country").desc())
)
