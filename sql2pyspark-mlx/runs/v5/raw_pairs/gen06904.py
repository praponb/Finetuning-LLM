from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 1))
    .withColumn("status_method_key", F.concat_ws("/", "status", "method"))
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

latest = (
    sessions_payments
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

gaps_2 = (
    latest
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments_2 = gaps_2.withColumn("session_no", F.sum("is_new_session").over(w5))

matched_keys = (
    sessions_payments_2
    .select("id", "status")
    .intersect(
        df_payments
        .filter(~F.col("method").isin(["card", "paypal", "bank_transfer"]))
        .select("id", "status")
    )
)

reconciled = (
    sessions_payments_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

stats = (
    reconciled
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.max("paid_at").alias("ended_at"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes_2"),
    )
    .filter(F.col("span_minutes_2") > 0)
)

result = (
    stats
    .select("quarter_start", "span_minutes", "span_minutes_2")
    .orderBy(F.col("span_minutes_2").desc())
)
