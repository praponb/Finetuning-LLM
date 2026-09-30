from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([24, 25]) & F.col("customer_id").isin([33, 35, 37, 40]))
    .select(
        "id",
        "amount",
        "customer_id",
        F.round((F.col("discount") * 100), 2).alias("discount_scaled"),
    )
    .orderBy("id")
    .limit(5)
)
