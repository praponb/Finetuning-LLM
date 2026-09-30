from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Books", "Sports", "Electronics"])
    .agg(F.avg("stock_qty"))
    .orderBy("brand")
)
