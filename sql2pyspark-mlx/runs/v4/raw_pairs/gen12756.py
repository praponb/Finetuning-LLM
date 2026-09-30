from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "method").orderBy("amount", "id")
w_ordered = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.orderBy("amount_bucket", "id")
w4 = Window.orderBy("row_seq", "id")
w5 = Window.partitionBy("method", "status").orderBy("amount", "id")
w6 = Window.partitionBy("status").orderBy("row_seq", "id")
w_part = Window.partitionBy("status", "method")
w8 = Window.partitionBy("method").orderBy("prev_amount", "id")
w9 = Window.partitionBy("method").orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method").orderBy("amount_bucket", "id")
w11 = Window.orderBy(F.col("amount_bucket_bucket").desc())
w12 = Window.partitionBy("order_id")
w13 = Window.partitionBy("method").orderBy(F.col("amount_bucket").desc_nulls_first())
w14 = Window.orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.partitionBy("status").orderBy(F.col("row_seq_bucket").asc_nulls_first())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

scored = (
    df_payments
    .select(
        "*",
        F.ntile(10).over(w_grp).alias("amount_bucket"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

scored_payments = (
    scored
    .select(
        "*",
        F.ntile(4).over(w3).alias("amount_bucket_bucket"),
        F.lag("amount", 1).over(w4).alias("prev_amount"),
        F.lead("amount_bucket", 1).over(w5).alias("next_amount_bucket"),
    )
)

scored_payments_2 = (
    scored_payments
    .select(
        "*",
        F.ntile(10).over(w6).alias("row_seq_bucket"),
        F.count("next_amount_bucket").over(w_part).alias("status_count_next_amount_bucket"),
        F.lag("next_amount_bucket", 2).over(w8).alias("prev_next_amount_bucket"),
        F.first("method", ignorenulls=True).over(w9).alias("first_known_method"),
        F.lead("next_amount_bucket", 2).over(w10).alias("next_next_amount_bucket"),
    )
)

with_windows = (
    scored_payments_2
    .select(
        "*",
        F.dense_rank().over(w11).alias("amount_bucket_bucket_dense_rank"),
        F.round((F.col("row_seq_bucket") / F.sum("row_seq_bucket").over(w12)), 4).alias("row_seq_bucket_share"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w13).alias("amount_bucket_cume_dist"),
        F.sum("amount_bucket_bucket").over(w14).alias("running_amount_bucket_bucket"),
        F.percent_rank().over(w15).alias("row_seq_bucket_percent_rank"),
    )
)

result = (
    enriched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "amount_bucket",
        "amount_bucket_bucket",
        "prev_amount",
        "next_amount_bucket",
        F.col("sum_shipment_cost"),
    )
)
