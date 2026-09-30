from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id")

result = (
    df_products
    .filter(~F.col("brand").isin(["Globex", "Initech"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
)
