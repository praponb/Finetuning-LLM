from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2023-09-26")
    .select(
        F.col("order_date").alias("event_date"),
        F.lit("order").alias("record_type"),
    )
    .unionByName(
        df_store_sales
        .select(
        F.col("sale_date").alias("event_date"),
        F.lit("store_sale").alias("record_type"),
    )
    )
    .orderBy("record_type")
)
