from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "lion city imports")
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "country", "rating")
)
