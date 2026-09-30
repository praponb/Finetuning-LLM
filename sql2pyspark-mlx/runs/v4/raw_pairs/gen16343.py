from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("id", "category", "price", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "category", "price")
)
