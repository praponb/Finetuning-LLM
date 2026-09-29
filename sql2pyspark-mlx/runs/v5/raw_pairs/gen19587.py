from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(8146, 10353))
    .select("id", "sqft")
    .unionByName(df_stores.filter(F.year("opened_date") == 2023).select("id", "sqft"))
    .distinct()
)
