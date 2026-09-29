from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .intersect(df_transactions.filter(F.col("is_flagged")).select("amount"))
    .orderBy(F.col("revenue").desc())
)
