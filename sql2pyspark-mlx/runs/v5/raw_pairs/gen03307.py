from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(~F.col("p.status").isin(["SUCCESS", "FAILED"]) & (F.col("p.order_id") == 21))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.concat_ws(",", F.col("p.method"), F.col("p.status")).alias("method_status_key"),
    )
    .orderBy(F.col("id").desc())
)
