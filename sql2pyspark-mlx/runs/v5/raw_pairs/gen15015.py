from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

with_latest = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.city",
        "s.segment",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        F.col("c.browser"),
        F.col("c.device").alias("session_device"),
    )
)

result = (
    with_latest
    .cube("city", F.date_trunc("year", "signup_date").alias("year_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
        F.avg(F.when(F.col("city") == "Bangkok", F.col("credit_limit"))).alias("bangkok_avg_credit_limit"),
    )
    .filter(F.col("consumer_credit_limit") >= 0)
    .orderBy(F.col("bangkok_avg_credit_limit").desc())
)
