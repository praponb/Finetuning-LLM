from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipping_fee").alias("order_shipping_fee"),
        F.col("c.channel"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isNull()), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isin([5, 12, 15])), "left_anti")
)

kept_customers = (
    kept
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left_anti")
)

result = (
    kept_customers
    .groupBy("channel", "country")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.round(F.avg("order_shipping_fee"), 2).alias("avg_order_shipping_fee"),
    )
    .filter(F.col("avg_order_shipping_fee") > 0)
    .orderBy(F.col("avg_order_shipping_fee").desc())
)
