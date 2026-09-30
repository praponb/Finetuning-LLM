from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Acme", "Globex"]) & F.col("brand").isNotNull())
)
