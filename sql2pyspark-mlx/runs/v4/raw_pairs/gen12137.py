from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "FAILED")
    .select("order_id", "status")
    .unionByName(
        df_payments
        .filter(F.col("order_id").isNull())
        .select("order_id", "status")
    )
    .distinct()
)
