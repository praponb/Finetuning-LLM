from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Berlin", "London", "New York", "Bangkok"])
    .agg(F.sum("sqft"))
)
