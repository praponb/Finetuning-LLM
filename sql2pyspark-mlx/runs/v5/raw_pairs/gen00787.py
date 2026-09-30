from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "customer_id").orderBy("session_id", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

metrics = (
    sessionized
    .select(
        "id",
        "customer_id",
        "country",
        "session_id",
        F.max("new_group_flag").over(w3).alias("last4_max_new_group_flag"),
        F.avg("new_group_flag").over(w3).alias("last4_avg_new_group_flag"),
        F.count("new_group_flag").over(w3).alias("last4_count_new_group_flag"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.session_id",
        "e.last4_max_new_group_flag",
        "e.last4_avg_new_group_flag",
        "e.last4_count_new_group_flag",
        F.col("d.name"),
    )
)

with_latest = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.session_id",
        "s.last4_max_new_group_flag",
        "s.last4_count_new_group_flag",
        "s.name",
        F.col("c.page").alias("web_event_page"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "session_id",
        "last4_max_new_group_flag",
        "last4_count_new_group_flag",
    )
)
