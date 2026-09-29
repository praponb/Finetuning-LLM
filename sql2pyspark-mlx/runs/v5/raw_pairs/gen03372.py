from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-05-08 18:00:00").cast("timestamp")) & (F.col("started_at") >= "2023-12-13"))
    .select("*", F.length("country").alias("country_length"))
)

not_flagged = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

latest = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

flagged = (
    latest
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

eligible = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_semi")
)

result = (
    eligible
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country_length",
        "session_no",
        F.when(F.col("browser").rlike("^(Chrome|Edge)$"), "CORE").otherwise("GROUP_B").alias("browser_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("country_length").desc(), "browser_zone")
)
