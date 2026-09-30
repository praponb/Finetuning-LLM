from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating")

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%IAM%"))
    .select("id", "country", "city", "rating", F.rank().over(w).alias("rating_rank"))
    .orderBy(F.col("rating_rank").desc())
)
