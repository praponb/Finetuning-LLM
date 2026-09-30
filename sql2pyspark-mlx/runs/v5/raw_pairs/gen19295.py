from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("units") < 35)
    .withColumn("units_scaled", F.round((F.col("units") * 100), 2))
)

product_id_summary = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.sum("units_scaled").alias("total_units_scaled"),
    )
)

result = (
    product_id_summary
    .filter(F.col("total_units_scaled") > 300)
    .select("product_id", "min_sale_date", "total_units_scaled")
    .orderBy("product_id")
)
