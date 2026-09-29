from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy(F.col("delivered_date").desc(), "id")

base = (
    df_shipments
    .filter(F.col("delivered_date") > "2023-07-01")
    .select(
        "*",
        F.length("carrier").alias("carrier_length"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
