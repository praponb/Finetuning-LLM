from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("city").isNotNull() & (F.col("signup_date") >= "2024-07-11"))
    .select(
        "*",
        F.initcap(F.trim("email")).alias("email_title"),
        F.ceil("credit_limit").alias("credit_limit_whole"),
    )
)

result = (
    base
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.city",
        "s.segment",
        "s.signup_date",
        F.col("c.device"),
        F.col("c.country").alias("session_country"),
    )
)
