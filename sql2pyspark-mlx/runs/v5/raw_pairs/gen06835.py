from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

ranked = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .withColumn("year_start", F.date_trunc("year", "shipped_date"))
    .rollup("year_start", "status")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").desc())
)
