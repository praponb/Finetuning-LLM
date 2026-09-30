from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2024-08-01")
    .groupBy(F.month("sale_date").alias("sale_month"), "store_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.min("units").alias("min_units"),
        F.countDistinct("units").alias("unique_products"),
    )
    .filter(F.col("min_units") < 37)
    .orderBy("sale_month", "store_id")
    .limit(50)
)
