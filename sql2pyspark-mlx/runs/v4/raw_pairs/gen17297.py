from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 5)
    .select("id", "units")
    .unionByName(
        df_store_sales
        .filter(F.month("sale_date").isin([4, 10, 12]))
        .select("id", "units")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
