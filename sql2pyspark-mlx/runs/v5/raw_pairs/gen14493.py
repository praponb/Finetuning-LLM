from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2100, 3300))
    .select("id", "units", "product_id")
    .subtract(
        df_store_sales
        .filter(F.month("sale_date").isin([2, 3, 6]))
        .select("id", "units", "product_id")
    )
)
