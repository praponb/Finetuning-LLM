from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

with_latest = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.segment",
        "s.credit_limit",
        F.col("c.country").alias("session_country"),
    )
)

result = (
    with_latest
    .select("id", "name", "segment", "credit_limit", "session_country")
    .orderBy(F.col("segment").desc_nulls_last())
)
