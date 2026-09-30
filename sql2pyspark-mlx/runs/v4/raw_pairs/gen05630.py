from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.trim("method").alias("clean_method"))
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy(F.col("year_start").desc())
)
