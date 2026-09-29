from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull())
    .groupBy("category")
    .pivot("brand", ["Acme", "Globex", "Initech"])
    .agg(F.count("cost"))
)
