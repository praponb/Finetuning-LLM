from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at_day").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("started_at").desc(), "id")

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.to_timestamp(F.lit("2023-11-30 20:00:00")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

scored = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

matched_keys = (
    scored
    .select("customer_id", "id")
    .exceptAll(
        df_sessions
        .filter(F.col("country").isin(["SG", "UK", "DE"]))
        .select("customer_id", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "started_at_day",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "GROUP_A").otherwise("EXTENDED").alias("device_zone"),
    )
    .orderBy(F.col("device_zone").desc_nulls_first())
)
