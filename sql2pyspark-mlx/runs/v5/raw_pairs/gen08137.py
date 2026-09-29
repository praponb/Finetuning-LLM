from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Home", "Books", "Sports"])
    .agg(F.avg("stock_qty"))
)
