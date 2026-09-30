from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 15))
    .select("status", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("order_id").isNull())
        .select("status", "order_id")
    )
)
