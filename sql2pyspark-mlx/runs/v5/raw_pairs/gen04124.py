from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & F.col("channel").isNotNull())
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
        F.date_sub("order_date", 83).alias("order_date_minus_83d"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "customer_id",
        F.col("amount").alias("AMOUNT"),
        F.col("shipping_fee").alias("SHIPPING_FEE"),
    )
    .unpivot(
        ids=["id", "customer_id"],
        values=["AMOUNT", "SHIPPING_FEE"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
