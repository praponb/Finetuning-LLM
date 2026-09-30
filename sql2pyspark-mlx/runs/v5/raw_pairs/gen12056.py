from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")

latest = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "country", "city")
)

result = (
    latest
    .select("id", "country", "city")
    .orderBy(F.col("city").desc_nulls_first(), "id")
)
