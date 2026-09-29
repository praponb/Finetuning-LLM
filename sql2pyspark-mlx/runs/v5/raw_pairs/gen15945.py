from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").asc_nulls_last())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-09-17").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .withColumn("greatest_discount_shipping_fee", F.greatest("discount", "shipping_fee"))
)

joined = (
    base
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.store_id",
        "a.order_date",
        "a.status",
        "a.channel",
        "a.discount",
        "a.shipping_fee",
        "a.greatest_discount_shipping_fee",
        F.col("p.credit_limit"),
        F.col("p.email"),
        F.col("p.city"),
    )
)

eligible = (
    joined
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") <= F.col("s.order_date")), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.carrier") == "DHL"), "left_anti")
)

latest = (
    kept
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "city",
        "row_num",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("min_account_balance").desc_nulls_last())
)
