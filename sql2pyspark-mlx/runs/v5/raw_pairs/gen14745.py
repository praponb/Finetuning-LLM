from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Acme", "Stark"]) & ((F.col("stock_qty") < 136) | F.col("brand").isNull()))
    .select(
        "product_name",
        "price",
        "category",
        F.dayofweek("launch_date").alias("launch_date_dayofweek"),
    )
)
