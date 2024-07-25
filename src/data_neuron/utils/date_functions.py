date_functions = {
    "postgres": {
        "CURRENT_DATE": {
            "Syntax": "CURRENT_DATE",
            "Returns": "Date",
            "Example": "SELECT CURRENT_DATE;"
        },
        "CURRENT_TIME": {
            "Syntax": "CURRENT_TIME",
            "Returns": "Time",
            "Example": "SELECT CURRENT_TIME;"
        },
        "CURRENT_TIMESTAMP": {
            "Syntax": "CURRENT_TIMESTAMP",
            "Returns": "Timestamp",
            "Example": "SELECT CURRENT_TIMESTAMP;"
        },
        "LOCALTIME": {
            "Syntax": "LOCALTIME",
            "Returns": "Time",
            "Example": "SELECT LOCALTIME;"
        },
        "LOCALTIMESTAMP": {
            "Syntax": "LOCALTIMESTAMP",
            "Returns": "Timestamp",
            "Example": "SELECT LOCALTIMESTAMP;"
        },
        "NOW": {
            "Syntax": "NOW()",
            "Returns": "Timestamp",
            "Example": "SELECT NOW();"
        },
        "AGE": {
            "Syntax": "AGE(timestamp1, timestamp2)",
            "Arguments": {
                "timestamp1": "A TIMESTAMP expression",
                "timestamp2": "A TIMESTAMP expression"
            },
            "Returns": "Interval",
            "Example": "SELECT AGE(NOW(), '1999-01-01');"
        },
        "DATE_PART": {
            "Syntax": "DATE_PART(field, source)",
            "Arguments": {
                "field": "A STRING expression",
                "source": "A DATE/TIMESTAMP expression"
            },
            "Returns": "Double precision",
            "Example": "SELECT DATE_PART('year', CURRENT_DATE);"
        },
        "DATE_TRUNC": {
            "Syntax": "DATE_TRUNC(precision, date)",
            "Arguments": {
                "precision": "A STRING expression",
                "date": "A DATE/TIMESTAMP expression"
            },
            "Returns": "Date/Timestamp",
            "Example": "SELECT DATE_TRUNC('month', CURRENT_DATE);"
        },
        "EXTRACT": {
            "Syntax": "EXTRACT(field FROM source)",
            "Arguments": {
                "field": "A STRING expression",
                "source": "A DATE/TIMESTAMP expression"
            },
            "Returns": "Double precision",
            "Example": "SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP);"
        },
        "TO_DATE": {
            "Syntax": "TO_DATE(text, format)",
            "Arguments": {
                "text": "A STRING expression",
                "format": "A STRING expression"
            },
            "Returns": "Date",
            "Example": "SELECT TO_DATE('2023-07-22', 'YYYY-MM-DD');"
        },
        "TO_TIMESTAMP": {
            "Syntax": "TO_TIMESTAMP(text, format)",
            "Arguments": {
                "text": "A STRING expression",
                "format": "A STRING expression"
            },
            "Returns": "Timestamp",
            "Example": "SELECT TO_TIMESTAMP('2023-07-22 10:23:54', 'YYYY-MM-DD HH24:MI:SS');"
        },
        "TO_CHAR": {
            "Syntax": "TO_CHAR(date, format)",
            "Arguments": {
                "date": "A DATE/TIMESTAMP expression",
                "format": "A STRING expression"
            },
            "Returns": "String",
            "Example": "SELECT TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD');"
        },
        "JUSTIFY_DAYS": {
            "Syntax": "JUSTIFY_DAYS(interval)",
            "Arguments": {
                "interval": "An INTERVAL expression"
            },
            "Returns": "Interval",
            "Example": "SELECT JUSTIFY_DAYS(INTERVAL '30 days');"
        },
        "JUSTIFY_HOURS": {
            "Syntax": "JUSTIFY_HOURS(interval)",
            "Arguments": {
                "interval": "An INTERVAL expression"
            },
            "Returns": "Interval",
            "Example": "SELECT JUSTIFY_HOURS(INTERVAL '25 hours');"
        },
        "JUSTIFY_INTERVAL": {
            "Syntax": "JUSTIFY_INTERVAL(interval)",
            "Arguments": {
                "interval": "An INTERVAL expression"
            },
            "Returns": "Interval",
            "Example": "SELECT JUSTIFY_INTERVAL(INTERVAL '1 mon 30 days');"
        },
        "ISFINITE": {
            "Syntax": "ISFINITE(date)",
            "Arguments": {
                "date": "A DATE/TIMESTAMP expression"
            },
            "Returns": "Boolean",
            "Example": "SELECT ISFINITE(DATE '2001-02-16');"
        },
        "LAST_DAY": {
            "Syntax": "LAST_DAY(date)",
            "Arguments": {
                "date": "A DATE expression"
            },
            "Returns": "Date",
            "Example": "SELECT LAST_DAY(DATE '1992-09-20');"
        },
        "MAKE_DATE": {
            "Syntax": "MAKE_DATE(year, month, day)",
            "Arguments": {
                "year": "An INTEGER expression",
                "month": "An INTEGER expression",
                "day": "An INTEGER expression"
            },
            "Returns": "Date",
            "Example": "SELECT MAKE_DATE(1992, 9, 20);"
        },
        "GREATEST": {
            "Syntax": "GREATEST(date1, date2)",
            "Arguments": {
                "date1": "A DATE expression",
                "date2": "A DATE expression"
            },
            "Returns": "Date",
            "Example": "SELECT GREATEST(DATE '1992-09-20', DATE '1992-03-07');"
        },
        "LEAST": {
            "Syntax": "LEAST(date1, date2)",
            "Arguments": {
                "date1": "A DATE expression",
                "date2": "A DATE expression"
            },
            "Returns": "Date",
            "Example": "SELECT LEAST(DATE '1992-09-20', DATE '1992-03-07');"
        }
    },
    "mysql": {
        "CURDATE()": {
            "Syntax": "CURDATE()",
            "Returns": "Date",
            "Example": "SELECT CURDATE();"
        },
        "CURTIME()": {
            "Syntax": "CURTIME()",
            "Returns": "Time",
            "Example": "SELECT CURTIME();"
        },
        "NOW()": {
            "Syntax": "NOW()",
            "Returns": "Datetime",
            "Example": "SELECT NOW();"
        },
        "DATE_FORMAT()": {
            "Syntax": "DATE_FORMAT(date, format)",
            "Arguments": {
                "date": "A DATE/TIMESTAMP expression",
                "format": "A STRING expression"
            },
            "Returns": "String",
            "Example": "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s');"
        },
        "DATE_ADD()": {
            "Syntax": "DATE_ADD(date, INTERVAL expr unit)",
            "Arguments": {
                "date": "A DATE expression",
                "expr": "An expression specifying the interval value to be added",
                "unit": "A unit keyword"
            },
            "Returns": "Date",
            "Example": "SELECT DATE_ADD('2023-07-22', INTERVAL 7 DAY);"
        },
        "DATE_SUB()": {
            "Syntax": "DATE_SUB(date, INTERVAL expr unit)",
            "Arguments": {
                "date": "A DATE expression",
                "expr": "An expression specifying the interval value to be subtracted",
                "unit": "A unit keyword"
            },
            "Returns": "Date",
            "Example": "SELECT DATE_SUB('2023-07-22', INTERVAL 7 DAY);"
        },
        "DATEDIFF()": {
            "Syntax": "DATEDIFF(date1, date2)",
            "Arguments": {
                "date1": "A DATE expression",
                "date2": "A DATE expression"
            },
            "Returns": "Integer",
            "Example": "SELECT DATEDIFF('2023-07-22', '2023-01-01');"
        },
        "STR_TO_DATE()": {
            "Syntax": "STR_TO_DATE(str, format)",
            "Arguments": {
                "str": "A STRING expression",
                "format": "A STRING expression"
            },
            "Returns": "Date",
            "Example": "SELECT STR_TO_DATE('22-07-2023', '%d-%m-%Y');"
        },
        "YEAR()": {
            "Syntax": "YEAR(date)",
            "Arguments": {
                "date": "A DATE expression"
            },
            "Returns": "Integer",
            "Example": "SELECT YEAR(NOW());"
        },
        "MONTH()": {
            "Syntax": "MONTH(date)",
            "Arguments": {
                "date": "A DATE expression"
            },
            "Returns": "Integer",
            "Example": "SELECT MONTH(NOW());"
        },
        "DAY()": {
            "Syntax": "DAY(date)",
            "Arguments": {
                "date": "A DATE expression"
            },
            "Returns": "Integer",
            "Example": "SELECT DAY(NOW());"
        }
    }
}
