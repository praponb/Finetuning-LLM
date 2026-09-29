from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("store_id", "customer_id").orderBy(F.col("amount").desc())
w_recent = Window.partitionBy("channel", "status")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-10-13")))
    .select(
        "*",
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("shipment_cost"))
)

remaining_keys = (
    latest_match
    .select("status")
    .subtract(
        df_orders
        .filter(F.col("status").isin(["PENDING", "SHIPPED", "COMPLETED"]))
        .select("status")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.rank().over(w2).alias("amount_rank"),
        F.corr("shipping_fee", "amount").over(w_recent).alias("corr_shipping_fee_amount"),
    )
)

result = (
    metrics
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee_int",
        "channel_clean",
        F.col("max_account_balance"),
    )
    .orderBy("customer_id")
)
