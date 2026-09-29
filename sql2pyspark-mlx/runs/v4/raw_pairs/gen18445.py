from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select("id", "status", "end_date", F.row_number().over(w).alias("row_num"))
)
