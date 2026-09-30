from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w3 = Window.partitionBy("store_id", "product_id")

result = (
    df_store_sales
    .withColumn("units_avg_2_prior", F.avg("units").over(w))
    .withColumn("next_revenue", F.lead("revenue", 2).over(w2))
    .withColumn("store_id_stddev_samp_units", F.stddev_samp("units").over(w3))
)
