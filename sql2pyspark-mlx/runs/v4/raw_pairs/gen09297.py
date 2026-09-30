from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1260)
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        F.date_add("order_date", 1).alias("order_date_plus_1d"),
    )
    .orderBy("order_date_plus_1d", F.col("id").desc())
)
