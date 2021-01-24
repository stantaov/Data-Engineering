class SqlQueries:
    fact_table_insert = ("""
        SELECT cicid, date_created, admission_number, port
        FROM i94
    """)

    visitors_table_insert = ("""
        SELECT  cicid, citizenship, resident, age, gender, birth_year, occupation
        FROM i94
    """)

    visit_table_insert = ("""
        SELECT cicid, allowed_stay_till, allowed_stay_days, stayed_days
        FROM i94
    """)

    visa_table_insert = ("""
        SELECT cicid, visa_class, visa_type, mode, visa_issued_by
        FROM i94
    """)

    flag_table_insert = ("""
        SELECT  cicid, arrival_flag, depart_flag, update_flag, match_flag
        FROM i94
    """)

    time_table_insert = ("""
        SELECT date_created, extract(hour from date_created), extract(day from date_created), extract(week from date_created), 
               extract(month from date_created), extract(year from date_created), extract(dayofweek from date_created)
        FROM i94
    """)