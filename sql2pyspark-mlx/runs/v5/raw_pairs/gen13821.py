from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select("plan")
    .distinct()
    .orderBy("plan")
)
