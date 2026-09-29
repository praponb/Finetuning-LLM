from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "status",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
    .orderBy(F.col("cost_share").desc())
)
