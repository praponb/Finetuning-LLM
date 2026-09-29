from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-10-01", "2025-05-01"))
    .select("id", "shipped_date", "cost", F.sum("cost").over(w).alias("running_cost"))
)
