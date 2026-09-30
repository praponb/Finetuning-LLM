from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("sqft") != 6276)
    .select("id", "region", "opened_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "region", "opened_date")
)
