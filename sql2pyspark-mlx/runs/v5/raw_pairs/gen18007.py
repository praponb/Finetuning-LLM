from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy(F.col("cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-07-15", "2023-08-31"))
    .select(
        "id",
        "carrier",
        "status",
        "cost",
        "shipped_date",
        F.last("shipped_date").over(w).alias("last_shipped_date"),
    )
    .orderBy("status")
)
