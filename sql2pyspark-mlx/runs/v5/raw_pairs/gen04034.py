from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 15481) & F.col("region").isin(["East", "Central"]) & (F.col("region") == "Central"))
)
