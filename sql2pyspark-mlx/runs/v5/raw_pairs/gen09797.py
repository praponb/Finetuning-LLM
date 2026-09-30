from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"), "store_id")
    .agg(
        F.max("units").alias("max_units"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 2)
    .orderBy("cnt")
)
