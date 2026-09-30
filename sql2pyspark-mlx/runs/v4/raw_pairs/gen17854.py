from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("units", "id")

result = (
    df_store_sales
    .withColumn("last_known_revenue", F.last("revenue", ignorenulls=True).over(w))
    .withColumn("next_revenue", F.lead("revenue", 2, 0).over(w2))
)
