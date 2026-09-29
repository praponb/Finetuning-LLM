from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 35) & (F.col("stock_qty") >= 313))
    .groupBy("brand")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.sum(F.when(F.col("category") == "Sports", F.col("cost")).otherwise(0)).alias("sports_cost"),
    )
    .filter(F.col("total_cost") >= 2000)
    .orderBy("brand")
    .limit(10)
)
