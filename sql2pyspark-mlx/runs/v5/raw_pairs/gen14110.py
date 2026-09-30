from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([8, 13, 15, 23]))
    .select(F.lit("order").alias("record_type"), "amount")
    .unionByName(
        df_products
        .select(F.lit("product").alias("record_type"), F.col("cost").alias("amount"))
    )
    .distinct()
    .orderBy("amount")
)
