from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2025-05-13", "2025-06-01"))
    .select(
        "id",
        "status",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
)
