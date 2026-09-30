from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Sports", "Electronics", "Fashion"])
    .agg(F.count("stock_qty"))
)
