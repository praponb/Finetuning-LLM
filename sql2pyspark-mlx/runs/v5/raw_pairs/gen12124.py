from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 5)
    .select("order_date", "customer_id")
    .unionByName(
        df_orders
        .filter(F.month("order_date").isin([6, 9, 12]))
        .select("order_date", "customer_id")
    )
    .distinct()
    .orderBy(F.col("order_date").desc())
)
