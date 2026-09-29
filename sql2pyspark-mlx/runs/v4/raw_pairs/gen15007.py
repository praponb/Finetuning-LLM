from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-07-21 04:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .withColumn("device_upper", F.upper("device"))
    .withColumn("country_clean", F.coalesce("country", F.lit("DIRECT")))
)

kept = (
    valid
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_semi")
)

gaps = (
    kept
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "device_upper",
        "country_clean",
        "is_new_burst",
        "session_id",
        F.when(F.col("browser").rlike("^(Chrome|Edge)$"), "OTHER_ZONE").when(F.col("browser").rlike("^(Safari|Firefox)$"), "GROUP_B").otherwise("EXTENDED").alias("browser_zone"),
        F.col("min_order_discount"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
