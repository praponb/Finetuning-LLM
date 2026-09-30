from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("status", "customer_id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w6 = Window.orderBy("store_id_sum_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("status_count_discount", "id")
w8 = Window.partitionBy("store_id").orderBy("store_id_count_shipping_fee", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("store_id", "status").orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2024-02-10").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.least("amount", "discount").alias("least_amount_discount"))
)

scored = (
    cleaned
    .select(
        "*",
        F.ntile(4).over(w_part).alias("shipping_fee_bucket"),
        F.count("discount").over(w2).alias("status_count_discount"),
        F.row_number().over(w_part).alias("seq_no"),
        F.corr("shipping_fee", "amount").over(w3).alias("corr_shipping_fee_amount"),
        F.sum("shipping_fee").over(w4).alias("store_id_sum_shipping_fee"),
    )
)

scored_orders = (
    scored
    .withColumn("amount_bucket", F.ntile(3).over(w5))
    .withColumn("store_id_count_shipping_fee", F.count("shipping_fee").over(w4))
)

scored_orders_2 = (
    scored_orders
    .select(
        "*",
        F.sum("corr_shipping_fee_amount").over(w6).alias("running_corr_shipping_fee_amount"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

enriched = (
    scored_orders_2
    .withColumn("status_count_discount_avg_2", F.avg("status_count_discount").over(w8))
    .withColumn("cumulative_discount", F.sum("discount").over(w9))
)

kept = (
    enriched
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < F.col("s.order_date")), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("running_corr_shipping_fee_amount") >= 4, "LARGE").otherwise("SMALL").alias("running_corr_shipping_fee_amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.max(F.when(F.col("status") == "RETURNED", F.col("seq_no"))).alias("returned_max_seq_no"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("cumulative_discount")).otherwise(0)).alias("mobile_cumulative_discount"),
    )
    .orderBy("returned_max_seq_no")
)
