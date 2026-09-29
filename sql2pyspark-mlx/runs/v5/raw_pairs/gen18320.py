from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 5, 6]))
    .select(F.col("last_restock").alias("event_date"), F.lit("inventory").alias("kind"))
    .unionByName(
        df_customers
        .filter(F.col("segment") == "Consumer")
        .select(F.col("signup_date").alias("event_date"), F.lit("customer").alias("kind"))
    )
    .distinct()
    .orderBy("event_date")
)
