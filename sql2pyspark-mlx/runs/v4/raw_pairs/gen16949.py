from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([1, 9, 16, 24]))
    .select("id", "order_id")
    .intersect(
        df_payments
        .filter(F.col("status") == "SUCCESS")
        .select("id", "order_id")
    )
)
