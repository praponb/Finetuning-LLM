from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(5493, 8641))
    .select("store_name", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") >= "2023-09-01")
        .select("store_name", "opened_date")
    )
    .distinct()
)
