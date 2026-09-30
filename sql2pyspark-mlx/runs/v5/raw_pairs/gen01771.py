from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 15) & (F.col("method") == "cash"))
    .select(
        "id",
        "method",
        F.substring("method", 1, 2).alias("method_prefix"),
        F.concat_ws("-", "method", "status").alias("method_status_key"),
    )
)
