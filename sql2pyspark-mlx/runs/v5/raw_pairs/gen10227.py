from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Grocery", "Fashion", "Books", "Sports"])
    .agg(F.count("stock_qty"))
    .orderBy("brand")
)
