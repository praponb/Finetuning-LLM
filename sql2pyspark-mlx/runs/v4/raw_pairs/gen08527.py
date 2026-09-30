from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("activity_month", F.date_trunc("MONTH", "shipped_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "shipped_date"))
    .groupingSets(
        [["activity_month", "year_start"], ["activity_month"]],
        "activity_month",
        "year_start",
    )
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"),
    )
)
