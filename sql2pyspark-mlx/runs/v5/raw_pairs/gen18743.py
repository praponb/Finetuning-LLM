from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")

result = df_inventory.select("id", "quantity", F.ntile(10).over(w).alias("quartile"))
