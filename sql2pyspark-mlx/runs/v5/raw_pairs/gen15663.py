from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "cost",
        "delivered_date",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
)
