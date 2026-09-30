from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("revenue") >= 1500) & F.month("sale_date").isin([1, 6, 10])) | (F.col("revenue") >= 3400))
    .select("id", "product_id", "store_id")
    .orderBy(F.col("store_id").desc(), "id")
    .limit(5)
)
