from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Fashion")
    .groupBy("category")
    .pivot("brand", ["Stark", "Umbrella", "Initech", "Globex"])
    .agg(F.max("price"))
    .orderBy("category")
)
