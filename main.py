import time
import operator
import gspread
import requests
import datetime


def get_c2c_price(payment_method, limit):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = {
        "proMerchantAds": False,
        "page": 1,
        "rows": 2,
        "payTypes": [payment_method],
        "countries": [],
        "publisherType": None,
        "asset": "USDT",
        "fiat": "RUB",
        "transAmount": limit,
        "tradeType": "BUY"
    }
    headers = {
        "authority": "p2p.binance.com",
        "accept": "*/*",
        "accept-language": "ru,en-US;q=0.9,en;q=0.8,uk-UA;q=0.7,uk;q=0.6,ru-RU;q=0.5",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "en",
        "origin": "https://p2p.binance.com",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Linux",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
        "sec-fetch-site": "same-origin"
    }

    return requests.request("POST", url, json=payload, headers=headers).json()['data'][0]['adv']['price']


def get_spot_price(attempts=0):
    try:
        attempts += 1
        return float(requests.get("https://api.binance.com/api/v3/ticker/price?symbol=USDTRUB").json()['price'])
    except AttributeError:
        if attempts < 3:
            return get_spot_price(attempts)
        else:
            return None


def prepare_worksheet(timings, banks, worksheet):
    worksheet.update("B2:E2", [banks])
    worksheet.update("F2", "Limit")
    worksheet.update("B6:G6", [timings])
    print("Worksheet prepared. ")


def get_current_cell(worksheet):
    for row in range(7, 40):
        for column in ['B', 'C', 'D', 'E', 'F', 'G']:
            if not worksheet.acell(column + str(row)).value:
                return column, str(row)


def get_limit(worksheet):
    return worksheet.acell('F3').value


def get_payment_methods(worksheet, initial_methods):
    methods_dict = {}
    for bank, cell in initial_methods.items():
        cell = cell.split(',')
        methods_dict.update({bank: worksheet.acell(''.join(cell[0] + str(int(cell[1]) + 1))).value})
    return methods_dict


def enter_into_cell(worksheet, cell, value):
    worksheet.update(cell, value)
    save_last_cell(cell, datetime.datetime.now().strftime('%d.%m'))


def save_last_cell(cell, date):
    with open("last.txt", 'w') as fw:
        cell_column = cell[0]
        cell_row = cell[1:]
        fw.write(f"{date},{cell_column},{cell_row}")


def load_last_cell():
    with open("last.txt", 'r') as fr:
        return fr.read()


def clear_sheet(worksheet):
    worksheet.clear()


def enter_current_date(worksheet, row):
    enter_into_cell(worksheet, f"A{row}", datetime.datetime.now().strftime('%d.%m'))


def find_biggest_spread(row, worksheet):
    columns = ['B', 'C', 'D', 'E', 'F', 'G']
    values = worksheet.get_values(f"B{row}:G{row}")[0]
    values = [float(spread.replace(",", ".")) if spread else 0 for spread in values]
    columns_dict = (dict(zip(columns, values)))
    return max(columns_dict.items(), key=operator.itemgetter(1))[0]


def color_biggest_spread(row, worksheet):
    worksheet.format(f"{find_biggest_spread(7, worksheet)}{row}", {
        "backgroundColor": {
            "red": 100.0,
            "green": 0.0,
            "blue": 0.0
        }})


def day_loop(worksheet, payment_methods, row, date):
    today = datetime.datetime.now().strftime('%d.%m')
    enter_current_date(worksheet, row)
    while today == date:
        now = datetime.datetime.now()

        today = datetime.datetime.now().strftime('%d.%m')
        hour = now.hour + now.minute / 60

        timeframes = {4.0: "B", 8.0: "C", 12.0: "D", 16.0: "E", 20.0: "F", 24.0: "G"}
        next_iteration = min([timeframe for timeframe in timeframes if hour < timeframe], key=lambda x: abs(x - hour))
        cell = timeframes[next_iteration] + row

        print(f"{now.strftime('%d.%m %H:%M:%S')}. Sleeping for {int((next_iteration - hour) * 3600)} seconds")
        time.sleep((next_iteration - hour) * 3600)
        four_hours_loop(worksheet, cell, payment_methods)

    color_biggest_spread(row, worksheet)
    day_loop(worksheet, payment_methods, row + 1, datetime.datetime.now().strftime('%d.%m'))


def four_hours_loop(worksheet, cell, pay_methods):
    limit = get_limit(worksheet)
    spot_price = get_spot_price()
    methods = get_payment_methods(worksheet, pay_methods)
    c2c_price = [float(get_c2c_price(method, limit)) for method in methods if methods[method]]
    if c2c_price:
        spread = round(max(c2c_price) - spot_price, 3)
    else:
        spread = 0

    enter_into_cell(worksheet, cell, spread)


def main_loop():
    payment_methods = {"RosBank": "B,2", "Tinkoff": "C,2", "RaiffeisenBankRussia": "D,2", "QIWI": "E,2"}
    time_holders = ["4:00", "8:00", "12:00", "16:00", "20:00", "24:00"]

    sa = gspread.service_account(filename="service_account.json")
    sh = sa.open("binance")
    wks = sh.worksheet("main")
    while True:
        prepare_worksheet(time_holders, list(payment_methods.keys()), wks)
        last_cell = load_last_cell().split(",")
        if last_cell[1] == '38':
            clear_sheet(wks)
            continue
        else:
            day_loop(wks, payment_methods, last_cell[2], last_cell[0])


if __name__ == '__main__':
    main_loop()
