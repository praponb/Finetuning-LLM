from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "amount",
        "order_date",
        "store_id",
        F.date_add("order_date", 34).alias("order_date_plus_34d"),
    )
)
