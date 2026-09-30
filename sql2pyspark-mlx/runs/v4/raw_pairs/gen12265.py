from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount_rounded", "id")
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id")
w5 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.concat("method", F.lit(" - "), "status").alias("method_status"),
        F.round("amount", 1).alias("amount_rounded"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lead("amount_rounded", 1).over(w).alias("next_amount_rounded"),
        F.count("amount_rounded").over(w2).alias("amount_rounded_count_7d"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

gaps = (
    scored
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("new_group_flag").over(w5))

result = (
    sessions_payments
    .select(
        "id",
        "method",
        "status",
        "method_status",
        "amount_rounded",
        "next_amount_rounded",
        "amount_rounded_count_7d",
        "row_seq",
        "island_id",
        F.when(F.col("method").rlike("^(cash|bank_transfer)$"), "GROUP_A").when(F.col("method").rlike("^(card|paypal)$"), "GROUP_B").otherwise("EXTENDED").alias("method_zone"),
    )
    .orderBy(F.col("next_amount_rounded").desc(), "amount_rounded_count_7d")
)
