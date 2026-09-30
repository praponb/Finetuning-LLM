from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .select("id", "supplier_id", "cost", "category", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .orderBy("category")
)
