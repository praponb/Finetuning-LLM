from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount_filled").desc(), "id")
w2 = Window.partitionBy("status").orderBy("amount_filled", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("method").isin(["cash", "paypal"]) & (F.col("paid_at") > F.lit("2023-11-12 02:00:00").cast("timestamp")))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "method", "amount", "paid_at", "status", "amount_filled")
)

scored = (
    deduped
    .select(
        "*",
        F.lead("amount_filled").over(w2).alias("next_amount_filled"),
        F.lag("amount_filled").over(w2).alias("prev_amount_filled"),
        F.lead("amount").over(w2).alias("next_amount"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

gaps = (
    scored
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "method",
        "paid_at",
        "prev_amount_filled",
        "next_amount",
        "seq_no",
        F.sum("is_new_session").over(w4).alias("session_id"),
    )
)

result = (
    islands
    .select(
        "id",
        "method",
        "paid_at",
        "prev_amount_filled",
        "seq_no",
        "session_id",
        F.when(F.col("method").rlike("^(bank_transfer|cash)$"), "GROUP_B").otherwise("EXTENDED").alias("method_zone"),
    )
    .orderBy(F.col("prev_amount_filled").asc_nulls_last())
)
