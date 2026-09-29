from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "price",
        F.when(F.col("price") >= 1300, "High").when(F.col("price") >= 450, "Medium").otherwise("Low").alias("price_level"),
    )
)
