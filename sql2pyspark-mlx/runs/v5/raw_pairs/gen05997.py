from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("segment").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

eligible = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "left_anti")
)

deduped = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped_customers = (
    deduped
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
    .select("id", "name", "email", "city", "age")
)

with_latest = (
    deduped_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status"),
        F.col("c.opened_date").alias("account_opened_date"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") < "2024-10-01"), "left_anti")
)

result = (
    not_flagged
    .groupBy("status")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("age").alias("avg_age"),
        F.stddev_samp("age").alias("age_stddev"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("distinct_ids").desc())
)
