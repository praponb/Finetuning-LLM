from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w_recent = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w)), 4))
    .withColumn("next_revenue", F.lead("revenue", 2, 0).over(w_recent))
)
