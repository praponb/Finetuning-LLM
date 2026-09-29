from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") < "2023-11-13") & F.col("product_name").isNotNull() & (F.col("price") < 710))
    .select(
        "id",
        "launch_date",
        F.add_months("launch_date", 12).alias("launch_date_plus_months"),
        F.when(F.col("price") >= 1230, "High").when(F.col("price") >= 570, "Medium").otherwise("Low").alias("price_band"),
    )
)
