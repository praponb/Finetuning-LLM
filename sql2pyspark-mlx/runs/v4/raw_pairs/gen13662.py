from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Books") | (F.length("product_name") >= 5))
    .select("id", "supplier_id")
    .orderBy(F.col("supplier_id").desc(), "id")
    .limit(1)
)
