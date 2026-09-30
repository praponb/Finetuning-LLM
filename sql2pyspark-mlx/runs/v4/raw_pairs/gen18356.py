from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2024-04-11").cast("date")) & F.col("product_id").isin([7, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        F.least("units", "revenue").alias("least_units_revenue"),
        F.when(F.col("revenue") >= 3500, "High").when(F.col("revenue") >= 700, "Medium").otherwise("Low").alias("revenue_level"),
    )
)

result = (
    base
    .groupBy("store_id", "product_id")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("*").alias("row_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("units"), 2).alias("avg_units_2"),
    )
)
