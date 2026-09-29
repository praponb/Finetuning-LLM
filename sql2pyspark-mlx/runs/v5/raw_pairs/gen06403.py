from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        F.col("c.country").alias("session_country"),
        F.col("c.device"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("credit_limit") >= 14700, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
        "session_country",
    )
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
    )
    .orderBy(F.col("credit_limit_tier").asc_nulls_last())
)
