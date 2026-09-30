from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1560)
    .select("id", "method", "order_id")
    .subtract(
        df_payments
        .filter(F.col("status") != "FAILED")
        .select("id", "method", "order_id")
    )
    .orderBy("order_id")
)
