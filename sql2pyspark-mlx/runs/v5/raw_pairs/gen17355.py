from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.year("opened_date") >= 2025)
    .select("id", "sqft", F.row_number().over(w).alias("row_num"))
)
