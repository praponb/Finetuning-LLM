from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("store_name").like("Mal%"))
    .select("sqft", "store_name")
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("%Kios%"))
        .select("sqft", "store_name")
    )
    .distinct()
)
