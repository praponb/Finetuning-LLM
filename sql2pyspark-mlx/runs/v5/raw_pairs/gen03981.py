from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 19) & F.col("store_id").isin([2, 6]))
    .select(
        "id",
        "units",
        "sale_date",
        "revenue",
        F.floor("units").alias("units_whole"),
    )
    .orderBy("sale_date", "id")
    .limit(5)
)
