from pyspark.sql import functions as F

result = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.start_date") >= F.col("s.signup_date")), "left_semi")
)
