from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select("id", "plan", "end_date", "customer_id", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 1)
    .orderBy("plan", "id")
)
