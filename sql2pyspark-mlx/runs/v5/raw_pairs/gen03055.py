from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2100, 3700))
    .select("sale_date", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") < 2300)
        .select("sale_date", "product_id")
    )
    .orderBy("product_id")
)
