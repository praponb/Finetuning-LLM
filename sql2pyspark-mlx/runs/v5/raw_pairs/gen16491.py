from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_stores
    .select("id", "city", "opened_date", "sqft", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "city", "opened_date", "sqft")
    .orderBy(F.col("sqft").desc())
)
