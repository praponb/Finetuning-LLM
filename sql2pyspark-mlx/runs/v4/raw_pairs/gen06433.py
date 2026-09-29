from pyspark.sql import functions as F

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2024-03-08").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.floor("amount").alias("amount_whole"))
)

result = (
    valid
    .select("id", "status", "shipping_fee", "amount", "discount")
    .unpivot(
        ["id", "status"],
        ["shipping_fee", "amount", "discount"],
        "measure",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
