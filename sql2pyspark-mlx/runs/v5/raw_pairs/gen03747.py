from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .subtract(
        df_transactions
        .filter(~F.col("merchant").isin(["7-Eleven", "Grab", "Lazada"]))
        .select("amount")
    )
)
