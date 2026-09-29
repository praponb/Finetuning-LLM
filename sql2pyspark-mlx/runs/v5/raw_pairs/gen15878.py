from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        "device",
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
        F.min("started_at").alias("first_started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
    )
    .filter(F.col("edge_rows") >= 1)
)
