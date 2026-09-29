from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("store_id")

result = (
    df_store_sales
    .withColumn("running_units", F.sum("units").over(w))
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w_ordered))
)
