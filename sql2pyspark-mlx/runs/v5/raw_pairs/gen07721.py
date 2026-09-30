from pyspark.sql import functions as F

max_sqft = df_stores.filter(F.col("opened_date").between("2024-05-01", "2024-11-01")).agg(F.max("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") >= max_sqft)
    .select("id", "sqft")
    .orderBy(F.col("id").desc())
)
