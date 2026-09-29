from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-05-01", "2024-11-01"))
    .select("city", "store_name")
    .unionByName(
        df_stores
        .filter(F.col("store_name").isNotNull())
        .select("city", "store_name")
    )
    .distinct()
    .orderBy("store_name")
)
