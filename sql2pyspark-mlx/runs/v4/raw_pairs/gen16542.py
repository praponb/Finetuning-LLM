from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "cost",
        F.count("*").over(w).alias("status_count"),
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
    )
    .orderBy(F.col("id").desc(), F.col("status_count").desc())
)
