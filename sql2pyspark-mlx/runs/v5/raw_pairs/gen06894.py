from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy("amount", "id")
w_grp = Window.partitionBy("customer_id").orderBy("discount", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc_nulls_first())
w4 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("status").orderBy("discount", "id")

scored = (
    df_orders
    .select(
        "*",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.dense_rank().over(w3).alias("discount_dense_rank"),
        F.last("shipping_fee", ignorenulls=True).over(w4).alias("last_known_shipping_fee"),
        F.lead("amount", 2).over(w_running).alias("next_amount"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("next_amount") >= 1290, "LARGE").otherwise("SMALL").alias("next_amount_tier"),
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
        F.round(F.sum("last_known_shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("last_known_shipping_fee_per_partner"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
    )
    .orderBy(F.col("returned_count").desc_nulls_last())
)
