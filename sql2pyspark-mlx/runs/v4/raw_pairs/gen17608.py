from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("credit_limit")
w3 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment")

result = (
    df_customers
    .select(
        "*",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.rank().over(w2).alias("credit_limit_rank"),
        F.last("segment", ignorenulls=True).over(w3).alias("last_known_segment"),
        F.count("credit_limit").over(w4).alias("segment_count_credit_limit"),
    )
)
