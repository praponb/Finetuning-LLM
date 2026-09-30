from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("Lio%"))
    .select("id", "city", "rating", F.lag("rating", 2).over(w).alias("prev_rating"))
    .orderBy("city")
)
