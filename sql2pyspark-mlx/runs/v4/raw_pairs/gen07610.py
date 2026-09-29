from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-05-01", "2025-09-01"))
    .select("store_id", "units")
    .unionByName(
        df_store_sales
        .filter(F.col("units").between(12, 17))
        .select("store_id", "units")
    )
    .orderBy(F.col("units").desc())
)
