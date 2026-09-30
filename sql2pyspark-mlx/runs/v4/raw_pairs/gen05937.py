from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")

result = (
    df_transactions
    .select("id", "amount", F.ntile(4).over(w).alias("quartile"))
    .orderBy("quartile")
)
