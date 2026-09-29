from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(118, 200))
    .select(
        "id",
        "store_id",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("quantity_filled").desc(), "id")
    .limit(10)
)
