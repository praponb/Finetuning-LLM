from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["bank_transfer", "cash"]))
    .select("order_id", "method")
    .subtract(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("order_id", "method")
    )
)
