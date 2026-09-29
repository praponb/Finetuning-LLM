from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 15535)
    .select("id", "sqft", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("%Old%"))
        .select("id", "sqft", "opened_date")
    )
    .distinct()
)
