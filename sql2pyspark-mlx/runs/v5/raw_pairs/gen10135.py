from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("shipping_fee", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.partitionBy("channel").orderBy("discount", "id")
w6 = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")
w7 = Window.partitionBy("customer_id").orderBy("discount_avg_5", "id")
w_ordered = Window.partitionBy("customer_id")
w9 = Window.partitionBy("customer_id").orderBy(F.col("discount_avg_5").desc_nulls_first())
w10 = Window.partitionBy("channel").orderBy("shipping_fee_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_6"),
        F.avg("discount").over(w2).alias("discount_avg_5"),
        F.ntile(3).over(w3).alias("shipping_fee_bucket"),
        F.row_number().over(w4).alias("seq_no"),
        F.ntile(5).over(w5).alias("discount_bucket"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "customer_id",
        "channel",
        "discount_avg_5",
        "seq_no",
        "discount_bucket",
        "row_num",
        F.sum("discount_avg_5").over(w_ordered).alias("customer_id_sum_discount_avg_5"),
        F.rank().over(w9).alias("discount_avg_5_rank"),
        F.sum("seq_no").over(w10).alias("cumulative_seq_no"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.is_active"))
    .select("e.*", F.col("p.segment"))
)

result = (
    joined
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
    .filter(F.col("partner_rows") >= 1)
    .orderBy("mobile_rows")
)
