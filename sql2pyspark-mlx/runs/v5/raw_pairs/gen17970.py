from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_last())
w_ordered = Window.partitionBy("status")
w5 = Window.partitionBy("status", "order_id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

with_windows = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select(
        "*",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lead("cost").over(w).alias("next_cost"),
        F.row_number().over(w).alias("seq_no"),
        F.avg("cost").over(w2).alias("cost_avg_2"),
    )
)

kept_keys = (
    with_windows
    .select("status", "id")
    .intersectAll(
        df_shipments
        .filter(F.month("shipped_date").isin([2, 10, 11]))
        .select("status", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        "prev_cost",
        "next_cost",
        "seq_no",
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

kept_keys_2 = (
    base
    .select("id", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("carrier") == "Kerry")
        .select("id", "order_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("cost_percent_rank", F.percent_rank().over(w3))
    .withColumn("status_min_seq_no", F.min("seq_no").over(w_ordered))
    .withColumn("status_sum_next_cost", F.sum("next_cost").over(w5))
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "status",
        "prev_cost",
        "next_cost",
        "seq_no",
        "status_prefix",
        "cost_percent_rank",
        F.when(F.col("status").rlike("^(DELIVERED|IN_TRANSIT)$"), "EXTENDED").when(F.col("status").rlike("^(LOST|RETURNED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
        F.col("max_payment_amount"),
    )
)
