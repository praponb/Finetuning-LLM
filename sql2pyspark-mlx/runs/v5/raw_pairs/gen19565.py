from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-01-01")
    .select("sale_date", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("revenue") < 1900)
        .select("sale_date", "store_id")
    )
    .distinct()
    .orderBy(F.col("sale_date").desc())
)
