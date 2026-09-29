from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "order_id")
    .subtract(
        df_payments
        .filter(F.col("method") != "paypal")
        .select("id", "order_id")
    )
)
