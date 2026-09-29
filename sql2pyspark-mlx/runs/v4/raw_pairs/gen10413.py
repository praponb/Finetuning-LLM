from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city", "segment").orderBy("age", "id")

result = (
    df_customers
    .select(
        "*",
        F.last("segment", ignorenulls=True).over(w).alias("last_known_segment"),
        F.lag("credit_limit", 1).over(w_grp).alias("prev_credit_limit"),
    )
)
