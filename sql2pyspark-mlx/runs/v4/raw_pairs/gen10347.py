from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id")
w3 = Window.orderBy("shipping_fee", "id")
w4 = Window.partitionBy("channel")
w_running = Window.partitionBy("customer_id").orderBy(F.col("quantity").desc_nulls_last())
w6 = Window.partitionBy("region").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy(F.col("quantity").desc())
w8 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("customer_id", "channel")

joined = (
    df_orders
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.region"), F.col("p.store_name"))
)

kept = (
    joined
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") <= F.col("s.order_date")), "left_semi")
)

enriched = (
    kept
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.store_name").alias("store_store_name"),
        F.col("d.city"),
        F.col("d.sqft"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

with_windows = (
    latest_match
    .select(
        "*",
        F.avg("quantity").over(w2).alias("customer_id_avg_quantity"),
        F.lead("discount", 2).over(w3).alias("next_discount"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
        F.rank().over(w_running).alias("quantity_rank"),
        F.sum("sqft").over(w6).alias("running_sqft"),
    )
)

with_windows_orders = (
    with_windows
    .withColumn("quantity_rank_2", F.rank().over(w7))
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w8))
    .withColumn("customer_id_avg_amount_share", F.avg("amount_share").over(w9))
)

result = (
    with_windows_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "region",
        "store_name",
    )
    .orderBy("customer_id")
)
