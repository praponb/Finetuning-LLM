from pyspark.sql import functions as F

result = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.start_date") >= F.col("s.signup_date")), "left_anti")
)
