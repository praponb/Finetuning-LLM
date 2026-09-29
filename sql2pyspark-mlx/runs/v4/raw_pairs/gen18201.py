from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("country", "store_id").orderBy("amount", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("credit_limit", "id")
w5 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("shipping_fee", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-08").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.trim("channel").alias("clean_channel"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.credit_limit"), F.col("p.country"))
)

eligible = (
    enriched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id") == 36), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date"))
)

eligible_orders = (
    latest_match
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status") == "IN_TRANSIT"), "left_semi")
)

scored = (
    eligible_orders
    .select(
        "*",
        F.avg("discount").over(w_part).alias("discount_avg_2_prior"),
        F.sum("shipping_fee").over(w3).alias("running_shipping_fee"),
        F.lead("amount", 1).over(w4).alias("next_amount"),
        F.sum("credit_limit").over(w5).alias("running_credit_limit"),
        F.avg("discount").over(w_recent).alias("discount_avg_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "discount",
        "shipping_fee",
        "clean_channel",
        "channel_filled",
        "country",
        F.when(F.col("channel").rlike("^(web|store)$"), "GROUP_B").when(F.col("channel").rlike("^(mobile|partner)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("channel_zone"),
    )
)
