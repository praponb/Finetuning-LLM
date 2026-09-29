from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w))
    .withColumn("last_known_country_2", F.last("country", ignorenulls=True).over(w2))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_grp))
)
