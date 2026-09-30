from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select("id", "city", "opened_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "city", "opened_date")
    .orderBy(F.col("city").desc())
)
