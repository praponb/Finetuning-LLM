from pyspark.sql import functions as F

avg_sqft = df_stores.filter(F.col("city").isin(["New York", "Singapore", "Berlin"])).agg(F.avg("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") >= avg_sqft)
    .select("id", "sqft")
    .orderBy("id")
)
