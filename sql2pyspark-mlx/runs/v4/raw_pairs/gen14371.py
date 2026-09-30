from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .unionByName(
        df_store_sales
        .filter(F.col("revenue") >= 2400)
        .select(F.col("revenue").alias("cost"))
    )
    .distinct()
    .orderBy("cost")
)
