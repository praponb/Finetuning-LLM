from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Chrome", "Safari"]))
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .orderBy(F.col("device").desc(), "browser")
)
