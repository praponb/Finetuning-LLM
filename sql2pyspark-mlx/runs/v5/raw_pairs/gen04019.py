from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country", "segment").orderBy("age", "id")
w3 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.orderBy("age", "id")
w5 = Window.partitionBy("segment", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_customers
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lead("credit_limit", 1).over(w2).alias("next_credit_limit"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_5"),
        F.lead("credit_limit", 2).over(w4).alias("next_credit_limit_2"),
        F.first("city", ignorenulls=True).over(w5).alias("first_known_city"),
    )
)
