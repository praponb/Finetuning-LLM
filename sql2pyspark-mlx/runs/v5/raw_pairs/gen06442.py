from pyspark.sql import functions as F

base = df_products.filter(F.col("category") != "Sports")

summary = (
    base
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", F.col("stock_qty")).otherwise(0)).alias("electronics_stock_qty"),
    )
)

result = (
    summary
    .select("brand", "electronics_stock_qty")
    .orderBy(F.col("electronics_stock_qty").desc(), "brand")
    .limit(1)
)
