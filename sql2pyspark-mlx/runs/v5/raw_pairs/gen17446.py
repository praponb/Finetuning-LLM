from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([2, 4, 5, 17]) & F.hour("paid_at").between(10, 14) & (F.col("amount") < 1040))
    .select(
        "order_id",
        "paid_at",
        F.round((F.col("amount") * 1.07), 2).alias("amount_scaled"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
    .orderBy(F.col("amount_scaled").desc())
)
