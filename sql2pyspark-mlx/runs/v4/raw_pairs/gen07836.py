from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("units").between(19, 31))
        .select("store_id", "product_id")
    )
)
