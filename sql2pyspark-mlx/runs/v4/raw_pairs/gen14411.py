from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-05-15").cast("date")) & (F.col("shipping_fee") < 17))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.col("amount").cast("int").alias("amount_int"),
        F.concat("channel", F.lit(", "), "status").alias("channel_status"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id").isin([1, 39])), "left_anti")
)
