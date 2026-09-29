from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 7) & (F.col("category") != "Fashion"))
    .agg(
        F.sum("price").alias("total_price"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc())
)
