from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") > 3700)
        .select("product_id")
    )
)
