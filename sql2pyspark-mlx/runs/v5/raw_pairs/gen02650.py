from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("store_id")

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-10-27", "2024-05-02"))
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_units", F.sum("total_units").over(w))
    .orderBy(F.col("cnt").desc(), "store_id")
)
