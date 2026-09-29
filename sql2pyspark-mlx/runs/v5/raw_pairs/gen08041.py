from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Fashion", "Grocery", "Electronics", "Books"])
    .agg(F.max("stock_qty"))
    .orderBy("brand")
)
