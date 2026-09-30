from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull())
    .select("store_name", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("sqft") != 9689)
        .select("store_name", "opened_date")
    )
    .distinct()
    .orderBy(F.col("opened_date").desc())
)
