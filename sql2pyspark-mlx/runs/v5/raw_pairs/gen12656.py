from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .filter(F.col("order_id") == 33)
    .select(
        "id",
        "order_id",
        "status",
        F.lag("amount").over(w).alias("prev_amount"),
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "status",
        "prev_amount",
        "next_amount",
        "event_seq",
        "cumulative_amount",
        F.when(F.col("status").rlike("^(REFUNDED|SUCCESS)$"), "OTHER_ZONE").when(F.col("status").rlike("^(FAILED)$"), "CORE").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("next_amount").desc_nulls_first())
)
