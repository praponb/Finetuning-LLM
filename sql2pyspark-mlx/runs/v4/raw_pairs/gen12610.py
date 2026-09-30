from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.last("segment", ignorenulls=True).over(w2).alias("last_known_segment"),
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share_2"),
        F.sum("age").over(w3).alias("cumulative_age"),
        F.last("segment", ignorenulls=True).over(w4).alias("last_known_segment_2"),
    )
)
