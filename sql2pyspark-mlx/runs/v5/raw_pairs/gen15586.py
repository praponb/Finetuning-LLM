from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")
w_recent = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-07-04 00:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .withColumn("browser_group", F.when(F.col("browser").isin(["Firefox", "Edge"]), "Tier 1").otherwise("Other"))
)

result = (
    filtered
    .filter(F.col("country").isin(["TH", "US"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.row_number().over(w).alias("seq_no"),
        F.last("browser", ignorenulls=True).over(w_recent).alias("last_known_browser"),
    )
)
