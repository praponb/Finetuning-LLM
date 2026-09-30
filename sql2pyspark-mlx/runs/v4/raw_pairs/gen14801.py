from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc())

base = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-06-29").cast("date")) & F.col("carrier").isNotNull() & F.col("status").isin(["RETURNED", "DELIVERED", "IN_TRANSIT"]))
    .select(
        "*",
        F.when(F.col("carrier").isin(["UPS", "Kerry"]), "Tier 1").otherwise("Other").alias("carrier_group"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

result = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
