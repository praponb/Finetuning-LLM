from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_sessions
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.when(F.col("country").rlike("^(TH|DE)$"), "GROUP_B").when(F.col("country").rlike("^(US|SG)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("country_zone"),
        F.col("sum_order_discount"),
    )
)
