from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("plan") != "standard")
    .select("id", "start_date", F.row_number().over(w).alias("start_date_row"))
    .orderBy(F.col("id").desc(), F.col("start_date").desc())
)
