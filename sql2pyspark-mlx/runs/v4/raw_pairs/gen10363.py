from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Home", "Grocery", "Electronics"])
    .agg(F.count("stock_qty"))
)
