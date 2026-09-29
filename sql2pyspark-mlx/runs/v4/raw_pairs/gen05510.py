from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.hour("paid_at").between(11, 17) & F.col("method").isin(["paypal", "cash"])) | F.hour("paid_at").between(8, 12)))
    .select(
        "id",
        "amount",
        "order_id",
        "status",
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
    .orderBy(F.col("id").desc(), "status")
)
