from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 15157)
    .orderBy("city")
)
