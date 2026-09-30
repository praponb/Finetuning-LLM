from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel")
w2 = Window.partitionBy("store_id", "customer_id")
w3 = Window.partitionBy("channel").orderBy("discount", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("customer_id").orderBy("discount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel", "customer_id").orderBy("store_id_count_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc_nulls_last())
w_hist = Window.partitionBy("store_id")
w9 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("store_id_count_discount").asc_nulls_last())
w11 = Window.partitionBy("customer_id").orderBy("shipping_fee_rank", "id").rowsBetween(-3, Window.currentRow)
w12 = Window.partitionBy("status", "store_id").orderBy("order_item_unit_price", "id")
w13 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("store_id").isin([3, 21, 22]))
    .withColumn("discount_adjusted", F.round((F.col("discount") * 0.9), 2))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("NONE")))
)

with_windows = (
    valid
    .withColumn("customer_id_sum_discount_adjusted", F.sum("discount_adjusted").over(w))
    .withColumn("store_id_count_discount", F.count("discount").over(w2))
    .withColumn("discount_bucket", F.ntile(5).over(w3))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

metrics = (
    with_latest
    .withColumn("last_known_discount_bucket", F.last("discount_bucket", ignorenulls=True).over(w5))
    .withColumn("last_known_discount", F.last("discount", ignorenulls=True).over(w6))
    .withColumn("shipping_fee_rank", F.rank().over(w7))
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w_hist))
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("discount").over(w9).alias("running_discount"),
        F.dense_rank().over(w10).alias("store_id_count_discount_dense_rank"),
        F.avg("store_id_count_discount").over(w11).alias("store_id_count_discount_avg_3"),
        F.lead("last_known_discount_bucket", 2).over(w12).alias("next_last_known_discount_bucket"),
        F.first("discount_bucket", ignorenulls=True).over(w13).alias("first_known_discount_bucket"),
    )
)

attributed = (
    enriched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "discount_adjusted",
        "customer_id_sum_discount_adjusted",
        F.when(F.col("status").rlike("^(RETURNED|COMPLETED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
