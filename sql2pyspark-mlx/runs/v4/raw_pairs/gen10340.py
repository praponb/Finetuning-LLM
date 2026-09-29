from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
w2 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id", "store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("order_date", "id")
w_hist = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc_nulls_last())
w6 = Window.partitionBy("store_id", "channel").orderBy("running_amount", "id")
w7 = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel", "status").orderBy(F.col("running_amount").asc_nulls_last())
w9 = Window.orderBy("shipping_fee_rank", "id")
w10 = Window.orderBy(F.col("amount").asc_nulls_last())
w11 = Window.partitionBy("store_id").orderBy("running_amount_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("discount", "id").rowsBetween(-3, -1)
w13 = Window.partitionBy("channel", "status").orderBy(F.col("next_discount").desc())

latest = (
    df_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.sum("amount").over(w2).alias("running_amount"),
        F.first("discount", ignorenulls=True).over(w_part).alias("first_known_discount"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.lead("discount", 2).over(w4).alias("next_discount"),
        F.rank().over(w_hist).alias("shipping_fee_rank"),
        F.ntile(5).over(w6).alias("running_amount_bucket"),
        F.first("first_known_discount", ignorenulls=True).over(w7).alias("first_known_first_known_discount"),
        F.rank().over(w8).alias("running_amount_rank"),
    )
)

enriched = (
    metrics
    .withColumn("prev_shipping_fee", F.lag("shipping_fee", 1).over(w9))
    .withColumn("amount_cume_dist", F.cume_dist().over(w10))
    .withColumn("last_known_running_amount_bucket", F.last("running_amount_bucket", ignorenulls=True).over(w11))
    .withColumn("first_known_discount_avg_3_prior", F.avg("first_known_discount").over(w12))
    .withColumn("next_discount_rank", F.rank().over(w13))
)

kept = (
    enriched
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") < F.col("s.order_date")), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.round(F.avg("first_known_first_known_discount"), 2).alias("avg_first_known_first_known_discount"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
    )
)
