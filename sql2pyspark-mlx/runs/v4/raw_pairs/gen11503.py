from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Globex")
    .groupBy("supplier_id")
    .pivot("brand", ["Initech", "Umbrella", "Stark"])
    .agg(F.sum("price"))
    .orderBy("supplier_id")
)
