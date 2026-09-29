from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("end_date")
    .subtract(df_shipments.select("delivered_date"))
)
