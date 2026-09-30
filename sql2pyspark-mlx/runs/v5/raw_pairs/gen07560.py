from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "cost", "status", "status_title")
)
