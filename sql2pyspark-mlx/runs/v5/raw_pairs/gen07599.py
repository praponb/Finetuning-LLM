from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 7, 8]))
    .select(
        "store_id",
        "revenue",
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
        F.round((F.col("units") * 1.07), 2).alias("units_adjusted"),
    )
)
