from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("status").orderBy("amount", "id")
w3 = Window.orderBy("order_date", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-04-20").cast("date"))
    .withColumn("days_since_order_date", F.datediff(F.current_date(), "order_date"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("shipment_cost"))
)

scored = (
    with_latest
    .withColumn("next_shipping_fee", F.lead("shipping_fee").over(w_running))
    .withColumn("prev_shipping_fee", F.lag("shipping_fee").over(w_running))
    .withColumn("next_shipment_cost", F.lead("shipment_cost").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("prev_shipping_fee_2", F.lag("shipping_fee", 2).over(w3))
)

enriched = (
    scored
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.order_date",
        "a.status",
        "a.amount",
        "a.shipping_fee",
        "a.days_since_order_date",
        "a.prev_shipping_fee",
        "a.next_shipment_cost",
        "a.prev_shipping_fee_2",
        F.col("p.country"),
    )
)

valid = (
    enriched
    .filter(F.col("country").isNotNull() & F.col("customer_id").isin([4, 9, 12]))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "days_since_order_date",
        "prev_shipping_fee",
        "next_shipment_cost",
        "prev_shipping_fee_2",
        "country",
        F.when(F.col("prev_shipping_fee") >= 40, "High").when(F.col("prev_shipping_fee") >= 11, "Medium").otherwise("Low").alias("prev_shipping_fee_band"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

result = (
    valid
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "days_since_order_date",
        "prev_shipping_fee",
        "next_shipment_cost",
        "prev_shipping_fee_2",
        "prev_shipping_fee_band",
        "resolved_country",
        F.col("max_account_balance"),
    )
    .orderBy(F.col("days_since_order_date").desc_nulls_first())
)
