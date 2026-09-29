from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("id", "category", "launch_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "category", "launch_date")
    .orderBy("id", "category")
)
