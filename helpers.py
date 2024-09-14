from datetime import datetime, date
from dateutil.relativedelta import relativedelta

def print_numbered_list(items, start=1):
    for i, item in enumerate(items, start):
        print(f"{i}. {item}")

def get_user_choice(prompt, valid_range):
    while True:
        try:
            choice = int(input(prompt))
            if choice in valid_range:
                return choice
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def get_user_input(prompt, input_type=str, validation_func=None):
    while True:
        try:
            user_input = input_type(input(prompt))
            if validation_func is None or validation_func(user_input):
                return user_input
            print("Invalid input. Please try again.")
        except ValueError:
            print(f"Please enter a valid {input_type.__name__}.")

def print_dataframe(df):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(df.to_string(index=False))

def print_divider(title):
    print("\n" + "=" * 50)
    print(title.center(50))
    print("=" * 50 + "\n")

def print_ascii_title():
    ascii_art = r"""
 ____            _            _   _                _____           _ 
|  _ \          | |          | | (_)              |_   _|         | |
| |_) |_   _  __| | __ _  ___| |_ _ _ __   __ _     | |  ___   ___| |
|  _ <| | | |/ _` |/ _` |/ _ \ __| | '_ \ / _` |    | | / _ \ / _ \ |
| |_) | |_| | (_| | (_| |  __/ |_| | | | | (_| |    | || (_) | (_)| |
|____/ \__,_|\__,_|\__, |\___|\__|_|_| |_|\__, |    \_/ \___/ \___|_|
                    __/ |                  __/ |                     
                   |___/                  |___/                      
 _           _____ ___ ____      
| |_ ___    |  ___|_ _|  _ \  ___ 
| __/ _ \   | |_   | || |_) |/ _ \
| || (_) |  |  _|  | ||  _ <|  __/
 \__\___/   |_|   |___|_| \_\____|
                                  
           By Vince Chen
    """
    print(ascii_art)

def get_user_specified_date():
    today = date.today()
    last_month = today - relativedelta(months=1)
    
    while True:
        year_input = input(f"Enter the year (YYYY) or press Enter for current year ({today.year}): ").strip()
        if year_input == "":
            year = today.year
        else:
            try:
                year = int(year_input)
                if not (1900 <= year <= 9999):
                    print("Invalid year. Please enter a year between 1900 and 9999.")
                    continue
            except ValueError:
                print("Invalid input. Please enter a valid year or press Enter.")
                continue

        month_input = input(f"Enter the month (1-12) or press Enter for previous month ({last_month.month}): ").strip()
        if month_input == "":
            month = last_month.month
        else:
            try:
                month = int(month_input)
                if not (1 <= month <= 12):
                    print("Invalid month. Please enter a month between 1 and 12.")
                    continue
            except ValueError:
                print("Invalid input. Please enter a valid month or press Enter.")
                continue

        return year, month

def validate_date(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return False