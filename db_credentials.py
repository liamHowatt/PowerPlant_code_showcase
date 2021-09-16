db_credentials = {
    "host": "localhost",
    "user": "", # Removed
    "passwd": "", # Removed
    "database": "powerplant_exp_unit_2"
}

uri = "{engine}://{user}:{passwd}@{host}:{port}/{schema}".format(
    engine="mysql",
    user=db_credentials["user"],
    passwd=db_credentials["passwd"],
    host=db_credentials["host"],
    port=3306,
    schema=db_credentials["database"],
)
