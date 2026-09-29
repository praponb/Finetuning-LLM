from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("island_id", "id")
w5 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w6 = Window.orderBy("session_start_flag", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-06-06 21:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("status_length", F.length("status"))
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

enriched = (
    sessions_payments
    .filter(F.col("island_id") > 7)
    .select(
        "id",
        "method",
        "paid_at",
        "session_start_flag",
        "island_id",
        F.count("island_id").over(w3).alias("island_id_count_90d"),
        F.lead("amount", 1, 0).over(w4).alias("next_amount"),
        F.row_number().over(w5).alias("seq_no"),
        F.avg("island_id").over(w6).alias("island_id_avg_6"),
    )
)

valid = (
    enriched
    .filter(F.to_date("paid_at") < "2025-04-01")
    .select(
        "id",
        "method",
        "paid_at",
        "island_id_count_90d",
        "next_amount",
        "seq_no",
        "island_id_avg_6",
        (F.col("island_id") * F.col("session_start_flag")).alias("island_id_times_session_start_flag"),
    )
)

result = (
    valid
    .select("id", "paid_at", "next_amount", "island_id_times_session_start_flag")
    .orderBy("next_amount")
)
