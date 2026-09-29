from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        "product_id",
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
    )
    .agg(F.avg("units").alias("avg_units"))
    .orderBy("avg_units")
)
