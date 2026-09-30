from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc_nulls_first(), "id")

base = (
    df_payments
    .filter((F.col("amount") <= 1350) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.lower("method").alias("method_lower"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

metrics = (
    base
    .filter(F.col("amount_filled") <= 52)
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "amount_filled",
        "method_lower",
        "method_filled",
        F.lag("amount").over(w).alias("prev_amount"),
        F.lag("amount_filled").over(w).alias("prev_amount_filled"),
        F.lead("amount_filled").over(w).alias("next_amount_filled"),
        F.row_number().over(w).alias("row_seq"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.order_date"))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .cube(
        F.col("d.channel").alias("channel"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.prev_amount")).alias("total_prev_amount"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
