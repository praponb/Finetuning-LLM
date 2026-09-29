from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").asc_nulls_first())
w3 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.cume_dist().over(w2).alias("credit_limit_cume_dist"),
        F.first("segment", ignorenulls=True).over(w3).alias("first_known_segment"),
    )
)
