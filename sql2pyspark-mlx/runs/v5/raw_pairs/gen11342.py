from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        "attributes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.size("items").alias("num_items"),
        F.col("ship_to.country").alias("ship_country"),
    )
)
