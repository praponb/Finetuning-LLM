from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Globex", "Initech"])
    .agg(F.avg("price"))
    .orderBy("category")
)
