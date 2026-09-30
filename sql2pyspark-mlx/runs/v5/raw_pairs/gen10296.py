from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Home", "Books", "Grocery", "Sports"])
    .agg(F.avg("stock_qty"))
    .orderBy("brand")
)
