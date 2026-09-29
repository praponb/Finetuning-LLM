from pyspark.sql import functions as F

eligible = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.status").isin(["RETURNED", "DELIVERED"])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)
