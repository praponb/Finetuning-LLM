from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_first(), "id")

eligible = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.signup_date")), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") > "2025-01-03"), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.start_date"))
)

kept_customers = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") >= F.col("s.start_date")), "left_semi")
)

latest_match = (
    kept_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.start_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser").alias("session_browser"))
)

deduped = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy("segment", "country")
    .pivot("session_browser", ["Firefox", "Edge"])
    .agg(F.sum("credit_limit").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "segment",
        "country",
        F.col("Firefox_total").alias("firefox_total"),
        F.col("Firefox_cnt").alias("firefox_cnt"),
        F.col("Edge_total").alias("edge_total"),
        F.col("Edge_cnt").alias("edge_cnt"),
    )
)
