from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") != "store")
    .select(F.col("order_date").alias("event_date"), F.lit("order").alias("source"))
    .unionByName(
        df_customers
        .select(F.col("signup_date").alias("event_date"), F.lit("customer").alias("source"))
    )
    .distinct()
    .orderBy("event_date")
)
