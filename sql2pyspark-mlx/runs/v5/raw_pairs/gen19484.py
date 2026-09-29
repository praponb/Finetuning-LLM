from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc_nulls_first(), "id")

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("device") != "tablet"))
    .select(
        "*",
        F.concat("browser", F.lit(" - "), "device").alias("browser_device"),
        F.substring("country", 1, 2).alias("country_prefix"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "browser_device",
        "country_prefix",
        "country_filled",
    )
)
