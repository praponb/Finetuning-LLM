from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "order_date",
        (F.col("discount") * F.col("amount")).alias("discount_times_amount"),
    )
    .orderBy(F.col("discount_times_amount").desc())
)
