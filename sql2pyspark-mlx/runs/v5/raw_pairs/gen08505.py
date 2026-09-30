from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")

result = (
    df_products
    .filter(F.col("cost").between(340, 690))
    .select("id", "brand", "cost", F.row_number().over(w).alias("rn"))
)
