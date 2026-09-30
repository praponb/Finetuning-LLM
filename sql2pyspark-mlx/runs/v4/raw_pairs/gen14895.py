from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 18)
    .select("id", "store_id", "units")
    .unionByName(
        df_store_sales
        .filter(F.col("units") <= 17)
        .select("id", "store_id", "units")
    )
    .orderBy("id")
)
