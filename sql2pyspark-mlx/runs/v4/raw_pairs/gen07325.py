from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("browser", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_sessions
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_running))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_grp))
    .withColumn("last_known_country_2", F.last("country", ignorenulls=True).over(w_part))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w_running))
)

result = (
    scored
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "last_known_country",
        "last_known_browser",
        "last_known_country_2",
        "first_known_country",
        F.when(F.col("device").rlike("^(tablet|mobile)$"), "GROUP_A").when(F.col("device").rlike("^(desktop)$"), "EXTENDED").otherwise("GROUP_B").alias("device_zone"),
    )
)
