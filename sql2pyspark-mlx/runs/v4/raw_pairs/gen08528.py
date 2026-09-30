from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("amount_with_shipping", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("customer_id").orderBy("discount", "id")
w5 = Window.partitionBy("status", "customer_id").orderBy(F.col("order_date").desc(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

valid = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2024-04-18"))) & F.col("customer_id").isNotNull())
    .withColumn("order_date_plus_months", F.add_months("order_date", 12))
    .withColumn("amount_with_shipping", (F.col("shipping_fee") * F.col("amount")))
)

scored = (
    valid
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_2"),
        F.sum("amount_with_shipping").over(w_grp).alias("cumulative_amount_with_shipping"),
    )
)

scored_orders = (
    scored
    .select(
        "*",
        F.max("amount_with_shipping").over(w3).alias("customer_id_max_amount_with_shipping"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

not_flagged = (
    scored_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isin([1, 21, 31])), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity") != 3), "left_anti")
)

kept_orders = (
    kept
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.delivered_date") <= F.col("s.order_date_plus_months")), "left_semi")
)

kept_keys = (
    kept_orders
    .select("store_id")
    .subtract(
        df_orders
        .filter(F.col("amount") >= 1150)
        .select("store_id")
    )
)

reconciled = (
    kept_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "order_date_plus_months",
        "amount_with_shipping",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("amount").asc_nulls_first())
)
