from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select("store_id", "product_id")
    .unionByName(
        df_store_sales
        .filter(F.month("sale_date").isin([1, 2, 8]))
        .select("store_id", "product_id")
    )
    .distinct()
    .orderBy("store_id")
)
