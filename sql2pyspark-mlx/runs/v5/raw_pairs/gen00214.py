from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("region").orderBy("rn", "id")

base = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "discount",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
        F.add_months("order_date", 3).alias("order_date_plus_months"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.region"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "store_id",
        "channel",
        "order_date_plus_months",
        F.lead("amount_with_shipping").over(w2).alias("next_amount_with_shipping"),
        F.lag("amount").over(w2).alias("prev_amount"),
        F.lead("amount").over(w2).alias("next_amount"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([22, 31])), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("WEEK", "order_date_plus_months").alias("week_start"))
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("partner_events").asc_nulls_last())
)
