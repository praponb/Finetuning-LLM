from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Umbrella", "Stark", "Acme", "Initech"])
    .agg(F.max("price"))
    .orderBy("category")
)
