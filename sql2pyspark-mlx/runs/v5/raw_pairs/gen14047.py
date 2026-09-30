from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(13, 28))
    .select(
        "discount",
        "shipping_fee",
        "amount",
        F.add_months("order_date", 3).alias("order_date_plus_months"),
    )
)
