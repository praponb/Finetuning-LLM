from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
