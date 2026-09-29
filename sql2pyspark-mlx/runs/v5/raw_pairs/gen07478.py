from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(F.lit("order").alias("origin"), "order_date")
    .union(
        df_store_sales
        .select(F.lit("store_sale").alias("origin"), "sale_date")
    )
    .distinct()
    .orderBy(F.col("order_date").desc())
)
