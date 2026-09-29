from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc())

flattened = df_price_tiers.select("id", "plan", F.explode("discounts").alias("element"))

result = (
    flattened
    .filter(F.col("element").between(37, 56))
    .select("id", "element", F.dense_rank().over(w).alias("element_dense_rank"))
)
