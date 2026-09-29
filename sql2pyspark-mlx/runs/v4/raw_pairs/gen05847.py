from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .filter(F.month("opened_date").isin([6, 10, 11]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "city", "opened_date", "region")
)
