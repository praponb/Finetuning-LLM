from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.filter(F.col("end_date") < "2023-05-21").alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_semi")
    .select(F.col("customers.id"), F.col("customers.country"), F.col("customers.name"))
    .orderBy("country")
)
