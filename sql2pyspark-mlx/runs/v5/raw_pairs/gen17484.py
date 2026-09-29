from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("shipped_date") >= 2023) | ~F.col("carrier").isin(["DHL", "Flash", "UPS"]))
    .groupBy("status")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy(F.col("max_cost").desc())
)
