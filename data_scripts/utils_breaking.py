import datetime

x = datetime.datetime.now()
LOGS_FILE = open(f'logs/log_{x.strftime("%Y_%m_%d")}', "a", buffering=1)
LOG_LVL_INFO = " - [INFO ] - "
LOG_LVL_WARNING = " - [WARN ] - "
LOG_LVL_FATAL = " - [FATAL] - "

def write_log(log_level, msg):
    LOGS_FILE.write(f'{datetime.datetime.now().strftime("%H:%M:%S")}{log_level}{msg}\n')

def db_key(db_type, db_size, intensity):
    if db_type == 1:
        return f"sql-{db_size}-{intensity}"
    elif db_type == 2:
        return f"sqlrep-{db_size}-{intensity}"
    elif db_type == 3:
        return f"spanner-{db_size}-{intensity}"

def doc_name(t, s):
    project_name = "break-data-bank"
    region = "us-west2"
    if t == "sql":
        if s == 1:
            return f"{project_name}:{region}:break-sm1"
        elif s == 2:
            return f"{project_name}:{region}:break-med1"
        elif s == 3:
            return f"{project_name}:{region}:break-lrg1"
        else:
            return None
    elif t == "sqlrep":
        if s == 1:
            return f"{project_name}:{region}:break-sm1-r"
        elif s == 2:
            return f"{project_name}:{region}:break-med1-r"
        elif s == 3:
            return f"{project_name}:{region}:break-lrg1-r"
        else:
            return None
    elif t == "spanner":
        if s == 1:
            return "break-sm1"
        elif s == 2:
            return "break-med1"
        elif s == 3:
            return "break-lrg1"
        else:
            return None
    else:
        return None

def db_int_from_key(key):
    if key == "sql":
        return 1
    elif key == "sqlrep":
        return 2
    elif key == "spanner":
        return 3
    return -1

def firestore_size_key(size_int):
    size_key = ""
    if size_int == 1:
        size_key = "1x"
    elif size_int == 2:
        size_key = "2x"
    elif size_int == 3:
        size_key = "4x"
    else:
        write_log(LOG_LVL_FATAL, "Couldn't determine a firestore key from the db_size. Unrecoverable error, ending run.")
        sys.exit(1)
    return size_key

