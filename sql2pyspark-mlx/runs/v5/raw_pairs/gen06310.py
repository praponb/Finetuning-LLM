from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "country", "rating")
)
