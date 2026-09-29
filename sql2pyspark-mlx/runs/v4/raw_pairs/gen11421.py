from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id")

result = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
