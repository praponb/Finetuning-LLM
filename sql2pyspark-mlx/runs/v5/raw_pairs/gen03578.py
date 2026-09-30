from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        "units",
        F.sum("revenue").over(w).alias("running_revenue"),
        F.rank().over(w2).alias("units_rank"),
    )
    .orderBy(F.col("id").desc())
)
