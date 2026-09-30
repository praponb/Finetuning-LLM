from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())
w2 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("revenue").asc_nulls_last())

result = (
    df_store_sales
    .withColumn("units_percent_rank", F.percent_rank().over(w))
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w2))
    .withColumn("revenue_percent_rank", F.percent_rank().over(w3))
)
