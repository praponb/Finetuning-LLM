from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id")

result = (
    df_products
    .filter(F.col("product_name").like("%mou%"))
    .select("id", "category", "cost", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "category", "cost")
)
