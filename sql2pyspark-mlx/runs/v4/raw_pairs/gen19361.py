from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "discount",
        "shipping_fee",
        F.col("amount").alias("AMOUNT_VAL"),
    )
    .unpivot(
        ["id", "customer_id"],
        ["discount", "shipping_fee", "AMOUNT_VAL"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
