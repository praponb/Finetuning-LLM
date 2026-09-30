from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 4, 8]))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"), "store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.countDistinct("units").alias("unique_stores"),
    )
    .orderBy(F.col("unique_stores").desc())
)
