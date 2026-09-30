from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 3) & ((F.col("product_id") == 5) | (F.col("units") < 26)))
    .select(
        "product_id",
        F.when(F.col("revenue") >= 3400, "High").when(F.col("revenue") >= 800, "Medium").otherwise("Low").alias("revenue_tier"),
    )
    .orderBy("product_id")
)
