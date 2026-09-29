from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .groupBy("category")
    .pivot("brand", ["Stark", "Acme"])
    .agg(F.count("cost"))
    .orderBy("category")
)
