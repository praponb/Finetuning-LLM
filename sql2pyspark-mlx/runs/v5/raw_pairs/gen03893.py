from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Stark", "Initech", "Globex", "Umbrella"])
    .agg(F.max("price"))
    .orderBy("category")
)
