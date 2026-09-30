from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel", "store_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w_running = Window.partitionBy("channel").orderBy("cost", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("customer_id", "channel").orderBy(F.col("amount").asc_nulls_last(), "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("amount_avg_2_prior").desc(), "id")

latest_match = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

scored = (
    latest_match
    .select(
        "*",
        F.last("shipping_fee", ignorenulls=True).over(w2).alias("last_known_shipping_fee"),
        F.corr("amount", "discount").over(w3).alias("corr_amount_discount"),
        F.avg("amount").over(w_running).alias("amount_avg_2_prior"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

latest_match_orders = (
    latest
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.channel",
        "s.discount",
        "s.corr_amount_discount",
        "s.amount_avg_2_prior",
        "s.rank_in_group",
        F.col("c.status").alias("shipment_status"),
    )
)

deduped = (
    latest_match_orders
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select("id", "shipment_status", "discount", "corr_amount_discount")
    .unpivot(
        ids=["id", "shipment_status"],
        values=["discount", "corr_amount_discount"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
