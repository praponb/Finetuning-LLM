from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Grocery")
    .select("cost")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 5, 6]))
        .select("revenue")
    )
    .orderBy("cost")
)
