from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1480)
    .select("id", "order_id")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([6, 36, 40]))
        .select("id", "order_id")
    )
)
