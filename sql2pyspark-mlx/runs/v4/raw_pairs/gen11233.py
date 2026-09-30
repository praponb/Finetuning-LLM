from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

valid = (
    df_shipments
    .filter(F.col("shipped_date") > F.to_date(F.lit("2023-03-05")))
    .select("*", F.initcap(F.trim("carrier")).alias("carrier_title"))
)

cleaned = (
    valid
    .filter(F.col("shipped_date") > F.lit("2023-05-24").cast("date"))
    .withColumn("status_carrier_key", F.concat_ws(",", "status", "carrier"))
)

enriched = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
    .withColumn("next_cost", F.lead("cost", 1).over(w3))
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"))
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "DHL", 1)), F.lit(0)), 2).alias("cost_per_dhl"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
    )
    .filter(F.col("fedex_count") >= 1)
)
