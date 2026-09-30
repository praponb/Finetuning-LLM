from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("launch_date").alias("event_date"), F.lit("product").alias("kind"))
    .unionByName(
        df_store_sales
        .select(F.col("sale_date").alias("event_date"), F.lit("store_sale").alias("kind"))
    )
    .distinct()
    .orderBy("event_date")
)
