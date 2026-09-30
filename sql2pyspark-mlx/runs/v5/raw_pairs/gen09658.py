from pyspark.sql import functions as F

min_sqft = df_stores.filter(F.col("opened_date").between("2023-07-01", "2024-05-01")).agg(F.min("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") == min_sqft)
    .select("id", "sqft", "city", "store_name")
    .orderBy(F.col("sqft").desc(), "city", "id")
    .limit(5)
)
