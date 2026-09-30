from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(20, 26))
    .select("sale_date", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([2, 3, 4, 5]))
        .select("sale_date", "store_id")
    )
    .orderBy(F.col("store_id").desc())
)
