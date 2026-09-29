from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Berlin", "London"]) & (F.col("sqft") >= 14552) & ~F.col("city").isin(["London", "Chiang Mai", "Berlin"]))
)
