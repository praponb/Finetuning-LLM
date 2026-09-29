from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1600, 2700))
    .select("store_id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([1, 5, 7, 9]))
        .select("store_id", "sale_date")
    )
    .distinct()
    .orderBy("sale_date")
)
