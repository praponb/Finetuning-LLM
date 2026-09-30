from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.26)
    .select("status", F.lit("order").alias("origin"))
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["promptpay", "card", "bank_transfer"]))
        .select("status", F.lit("payment").alias("origin"))
    )
    .orderBy("status")
)
