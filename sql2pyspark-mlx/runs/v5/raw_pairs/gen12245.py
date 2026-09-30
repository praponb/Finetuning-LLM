from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_name").orderBy("is_legal", "id")
w2 = Window.partitionBy("location").orderBy("is_legal", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.when(F.col("dept_name") == "Legal", 1).otherwise(0).alias("is_legal"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)

result = (
    valid
    .select(
        "*",
        F.lead("is_legal").over(w_grp).alias("next_is_legal"),
        F.lead("budget").over(w_grp).alias("next_budget"),
        F.lag("budget").over(w_grp).alias("prev_budget"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.last("dept_name", ignorenulls=True).over(w2).alias("last_known_dept_name"),
    )
)
