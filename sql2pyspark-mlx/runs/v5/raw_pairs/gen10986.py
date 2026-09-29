from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5, 6]))
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(
        F.count("*").alias("store_sale_count"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") < 3)
    .select("sale_year", "store_sale_count", "unique_stores")
    .orderBy(F.col("store_sale_count").desc())
)
