from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "segment").orderBy(F.col("age").desc_nulls_first())

result = (
    df_customers
    .select(
        "*",
        F.sum("age").over(w).alias("running_age"),
        F.dense_rank().over(w2).alias("age_dense_rank"),
    )
)
