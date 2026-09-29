from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["cash", "paypal"]))
    .select("id", "order_id", "method")
    .unionByName(
        df_payments
        .filter(F.col("amount").between(420, 690))
        .select("id", "order_id", "method")
    )
)
