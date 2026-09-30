from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select("id", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]))
        .select("id", "order_id")
    )
)
