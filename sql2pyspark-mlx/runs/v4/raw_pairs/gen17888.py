from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_last())
w2 = Window.partitionBy("method", "status").orderBy(F.col("shipping_fee").desc_nulls_first(), "id")

deduped = (
    df_payments
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_dims = (
    deduped
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.shipping_fee"), F.col("p.discount"), F.col("p.channel"))
)

deduped_payments = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_payments
    .cube(
        F.when(F.col("shipping_fee") >= 32, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("shipping_fee_tier").desc())
)
