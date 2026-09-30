from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")

result = (
    df_products
    .select("id", "brand", "cost", F.ntile(3).over(w).alias("cost_ntile"))
    .orderBy("brand")
)
