from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("region") == "West")
    .select("id", "city", "opened_date", F.ntile(2).over(w).alias("opened_date_ntile"))
)
