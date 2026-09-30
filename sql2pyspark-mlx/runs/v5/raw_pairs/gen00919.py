from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-05-30").cast("date"))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.length("device").alias("device_length"),
    )
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .groupingSets(
        [["activity_month", "country"], ["activity_month"], []],
        "activity_month",
        "country",
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.min("started_at").alias("first_seen"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .orderBy(F.col("distinct_customer_ids").desc())
)
