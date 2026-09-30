from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Stark"]) & (F.col("price") < 760))
    .select("id", "stock_qty", "cost")
    .orderBy("id")
)
