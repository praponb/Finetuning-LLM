from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())

result = (
    df_products
    .select("id", "supplier_id", "cost", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("supplier_id").desc(), F.col("id").desc())
)
