from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "method").orderBy(F.col("new_group_flag").asc_nulls_first(), "id")

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

base = (
    sessions_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-12-27 21:00:00"))) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "new_group_flag",
        "island_id",
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "new_group_flag",
        "paid_at_label",
        "method_filled",
        "rank_in_group",
        F.when(F.col("method").rlike("^(bank_transfer|card)$"), "OTHER_ZONE").when(F.col("method").rlike("^(paypal|cash)$"), "EXTENDED").otherwise("CORE").alias("method_zone"),
    )
)
