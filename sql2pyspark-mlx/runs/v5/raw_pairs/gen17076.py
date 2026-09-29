from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .pivot("category", ["Books", "Fashion", "Grocery", "Sports"])
    .agg(F.sum("price"))
    .orderBy("supplier_id")
)
