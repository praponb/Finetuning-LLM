from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("shipped_date").desc_nulls_first(), "id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-07-25"))) & F.col("carrier").isNotNull())
    .select("*", F.concat("carrier", F.lit(", "), "status").alias("carrier_status"))
)

ranked = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked_shipments = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered_shipments = (
    ranked_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("Kerry")))
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("carrier_clean", F.coalesce("carrier", F.lit("OTHER")))
)

result = (
    filtered_shipments
    .groupBy(F.date_trunc("YEAR", "shipped_date").alias("year_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("days_since_shipped_date", [0.25, 0.5, 0.75]).alias("days_since_shipped_date_quartiles"),
        F.round(F.sum("days_since_shipped_date") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("days_since_shipped_date_per_in_transit"),
    )
    .orderBy(F.col("row_count").desc())
)
