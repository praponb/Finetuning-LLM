from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("channel", "store_id").orderBy("shipping_fee", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("customer_id").orderBy("discount", "id")
w4 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("discount_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w9 = Window.partitionBy("status", "customer_id").orderBy(F.col("order_date").desc(), "id")

enriched = (
    df_orders
    .select(
        "*",
        F.avg("discount").over(w).alias("store_id_avg_discount"),
        F.avg("discount").over(w2).alias("discount_avg_6_prior"),
        F.lead("amount", 1).over(w3).alias("next_amount"),
        F.sum("amount").over(w4).alias("running_amount"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= F.col("s.order_date")), "left_anti")
)

enriched_orders = (
    eligible
    .select(
        "*",
        F.sum("next_amount").over(w5).alias("running_next_amount"),
        F.sum("shipping_fee").over(w6).alias("cumulative_shipping_fee"),
        F.corr("discount", "shipping_fee").over(w).alias("corr_discount_shipping_fee"),
        F.sum("discount").over(w7).alias("cumulative_discount"),
    )
)

ranked = (
    enriched_orders
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

ranked_orders = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    ranked_orders
    .filter(F.col("order_date") > F.lit("2024-02-24").cast("date"))
    .withColumn("status_group", F.when(F.col("status").isin(["CANCELLED", "COMPLETED"]), "Group A").otherwise("Other"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    valid
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "store_id_avg_discount",
        "discount_avg_6_prior",
        "next_amount",
    )
)
