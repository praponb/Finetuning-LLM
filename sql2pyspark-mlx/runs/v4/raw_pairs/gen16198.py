from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 23)
    .select("id", "end_date", F.row_number().over(w).alias("end_date_row"))
    .orderBy(F.col("end_date_row").desc())
)
