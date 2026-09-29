from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(~(F.col("units").between(29, 30) & (F.col("revenue").isNotNull() | F.col("revenue").between(2200, 2700))))
)

store_id_summary = (
    valid_store_sales
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    store_id_summary
    .filter(F.col("stddev_units") >= 52)
    .select("store_id", "last_sale_date", "stddev_units")
    .orderBy(F.col("stddev_units").desc(), F.col("store_id").desc())
)
