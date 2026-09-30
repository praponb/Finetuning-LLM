from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("sqft") >= 8875) & (F.col("city") == "New York")) | F.col("city").isin(["New York", "London", "Berlin"]))
    .select("id", "city", "opened_date", "region")
    .orderBy(F.col("id").desc(), F.col("city").desc())
)
