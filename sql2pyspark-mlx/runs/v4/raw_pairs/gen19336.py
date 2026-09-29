from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w))
    .withColumn("first_known_channel_2", F.first("channel", ignorenulls=True).over(w))
)
