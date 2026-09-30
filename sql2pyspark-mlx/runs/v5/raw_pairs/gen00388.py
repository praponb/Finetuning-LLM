from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("rating") > 2.5)
    .select("id", "country", "rating", F.row_number().over(w).alias("rating_row"))
)
