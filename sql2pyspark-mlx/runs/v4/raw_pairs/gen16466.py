from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("started_at") >= "2025-05-01")
        .select("country", F.lit("session").alias("source"))
    )
    .distinct()
    .orderBy(F.col("country").desc())
)
