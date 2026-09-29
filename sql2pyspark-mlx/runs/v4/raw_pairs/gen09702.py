from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("stock_qty") < 208) & (F.col("brand") == "Stark")) | F.col("cost").between(590, 650))
    .select("category", "supplier_id", "price")
    .orderBy(F.col("price").desc(), F.col("supplier_id").desc())
)
