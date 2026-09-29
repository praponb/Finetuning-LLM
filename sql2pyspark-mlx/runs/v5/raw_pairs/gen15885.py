from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("row_num", "id")
w4 = Window.partitionBy("store_id", "channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("customer_id").isNull())
    .select(
        "*",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity") < 7), "left_anti")
)

enriched = (
    eligible
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.name").like("FR%"))
    .select("s.*", F.col("b.country"), F.col("b.name"))
)

metrics = (
    enriched
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "discount",
        "days_since_order_date",
        "row_num",
        "name",
        F.row_number().over(w2).alias("event_seq"),
        F.lead("shipping_fee", 1, 0).over(w3).alias("next_shipping_fee"),
        F.sum("days_since_order_date").over(w4).alias("cumulative_days_since_order_date"),
    )
)

enriched_orders = (
    metrics
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.sqft"))
)

result = (
    enriched_orders
    .select(
        "id",
        "order_date",
        "discount",
        "days_since_order_date",
        "row_num",
        "name",
        "event_seq",
        "next_shipping_fee",
        "cumulative_days_since_order_date",
    )
)
