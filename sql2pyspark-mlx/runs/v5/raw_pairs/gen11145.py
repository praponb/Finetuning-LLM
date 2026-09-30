from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .select("id", "method", "paid_at", F.ntile(10).over(w).alias("paid_at_ntile"))
)
