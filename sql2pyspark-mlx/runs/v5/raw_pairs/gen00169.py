from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .groupingSets(
        [["year_start", "customer_id"], ["year_start"]],
        "year_start",
        "customer_id",
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
