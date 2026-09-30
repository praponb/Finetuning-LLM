from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("started_at").desc(), "id")

valid = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.concat_ws("-", "country", "browser").alias("country_browser_key"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_clean"),
    )
)

result = (
    valid
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.row_number().over(w2).alias("row_seq"),
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
    )
)
