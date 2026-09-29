from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-08-31")))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

result = (
    base
    .select("channel", "customer_id")
    .intersectAll(
        df_orders
        .filter(F.col("shipping_fee") < 13)
        .select("channel", "customer_id")
    )
)
