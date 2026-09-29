from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        "cost",
        "carrier",
        F.sum("cost").over(w).alias("running_cost"),
        F.ntile(3).over(w2).alias("quartile"),
    )
    .orderBy("cost")
)
