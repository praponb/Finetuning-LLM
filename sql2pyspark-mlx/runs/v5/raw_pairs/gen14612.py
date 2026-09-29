from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.col("product_name").like("Bac%") & (F.col("category") != "Electronics") & (F.col("price") > 1120)))
    .select("id", "supplier_id", "brand", F.floor("cost").alias("cost_whole"))
)
