from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 3, 9]))
    .select("id", "product_id", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([7, 9]))
        .select("id", "product_id", "store_id")
    )
    .distinct()
)
