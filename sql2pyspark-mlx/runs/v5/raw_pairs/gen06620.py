from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("G%"))
    .select("id", "city", "rating", F.rank().over(w).alias("rating_rank"))
    .orderBy(F.col("rating").desc(), "city")
)
