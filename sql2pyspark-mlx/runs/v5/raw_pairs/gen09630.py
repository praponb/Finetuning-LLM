from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .select(
        "id",
        "country",
        F.first("browser", ignorenulls=True).over(w_ordered).alias("first_known_browser"),
        F.first("country", ignorenulls=True).over(w_ordered).alias("first_known_country"),
    )
)

result = enriched.select("id", "first_known_browser", "first_known_country")
