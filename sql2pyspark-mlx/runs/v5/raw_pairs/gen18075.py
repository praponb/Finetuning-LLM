from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-06-22"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.substring("device", 1, 2).alias("device_prefix"),
    )
)

result = (
    base
    .select(
        "id",
        "device",
        "country",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country_2"),
    )
)
