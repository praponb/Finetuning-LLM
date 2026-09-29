from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-06-08"))) & F.col("method").isNotNull())
    .withColumn("paid_at_date", F.to_date("paid_at"))
    .withColumn("method_prefix", F.substring("method", 1, 3))
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .withColumn("activity_month", F.date_trunc("month", "paid_at"))
    .groupingSets(
        [["activity_month", "order_id"], ["activity_month"]],
        "activity_month",
        "order_id",
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.sum("session_no").alias("total_session_no"),
        F.size(F.collect_set("method")).alias("num_methods_2"),
    )
)
