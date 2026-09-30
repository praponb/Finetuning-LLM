from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_payments
        .filter(~F.col("status").isin(["SUCCESS", "FAILED"]))
        .select("status")
    )
    .distinct()
)
