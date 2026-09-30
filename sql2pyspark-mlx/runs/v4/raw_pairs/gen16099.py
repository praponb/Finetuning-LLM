from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .select("sale_date", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([1, 3, 4, 6]))
        .select("sale_date", "product_id")
    )
)
