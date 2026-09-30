from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")

result = df_accounts.select("id", "balance", F.ntile(10).over(w).alias("quartile"))
