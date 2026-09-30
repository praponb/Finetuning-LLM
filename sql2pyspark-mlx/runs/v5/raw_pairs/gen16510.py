from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.initcap(F.trim("country")).alias("country_title"),
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(10)
)
