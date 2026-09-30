from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.city",
        "s.segment",
        "s.signup_date",
        "s.is_active",
        "s.age",
        F.col("c.country").alias("session_country"),
        F.col("c.browser").alias("session_browser"),
    )
)

result = (
    latest_match
    .groupBy(F.when(F.col("age") >= 60, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.count("*").alias("event_count"),
        F.stddev_samp("age").alias("age_stddev"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .filter(F.col("age_stddev") < 1000000)
    .orderBy(F.col("age_stddev").asc_nulls_last())
)
