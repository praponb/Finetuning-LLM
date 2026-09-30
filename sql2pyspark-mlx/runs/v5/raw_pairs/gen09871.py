from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .pivot("brand", ["Globex", "Umbrella"])
    .agg(F.count("price"))
    .orderBy("category")
)
