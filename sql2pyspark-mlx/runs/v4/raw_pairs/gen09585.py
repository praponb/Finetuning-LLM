from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").asc_nulls_last())

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & ~F.col("status").isin(["LOST", "IN_TRANSIT", "DELIVERED"]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)

result = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
