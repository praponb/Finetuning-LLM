from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 32)
    .select(
        "id",
        "status",
        "order_id",
        F.concat("method", F.lit("/"), "status").alias("method_status"),
        F.when(F.col("method").isin(["card", "bank_transfer"]), "Group A").otherwise("Other").alias("method_group"),
    )
)
