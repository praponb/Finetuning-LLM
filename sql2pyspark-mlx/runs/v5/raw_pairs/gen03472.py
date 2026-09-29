from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")

result = (
    df_products
    .select("id", "brand", "cost", F.avg("cost").over(w).alias("brand_avg_cost"))
    .orderBy("brand")
)
