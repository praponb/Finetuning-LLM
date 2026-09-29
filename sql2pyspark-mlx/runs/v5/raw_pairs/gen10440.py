from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units").between(34, 40))
        .select("id")
    )
)
