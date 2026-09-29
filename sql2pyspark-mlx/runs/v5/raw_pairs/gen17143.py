from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 8618)
    .select("id", "store_name")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") >= "2025-09-01")
        .select("id", "store_name")
    )
    .distinct()
)
