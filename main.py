import time
import operator
import gspread
import requests
import datetime
import string


def get_spot_price(attempts=0):
    try:
        attempts += 1
        return float(requests.get("https://api.binance.com/api/v3/ticker/price?symbol=USDTRUB").json()['price'])
    except AttributeError:
        if attempts < 3:
            return get_spot_price(attempts)
        else:
            return None


class BinanceC2CScraper:
    def __init__(self, fiat, asset, limit, method, is_merchant, payment_methods):
        self.fiat = fiat
        self.asset = asset
        self.limit = limit
        self.is_merchant = is_merchant
        self.method = method
        self.payment_methods = payment_methods

        self.data = self.make_c2c_requests()

    def make_c2c_requests(self):
        for payment_method in self.payment_methods:
            url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
            payload = {
                "proMerchantAds": self.is_merchant,
                "page": 1,
                "rows": 2,
                "payTypes": [payment_method],
                "countries": [],
                "publisherType": None,
                "asset": self.asset,
                "fiat": self.fiat,
                "transAmount": self.limit,
                "tradeType": self.method
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

            yield requests.request("POST", url, json=payload, headers=headers).json()['data']

    def get_price(self):
        try:
            return float(next(self.data)[0]['adv']['price'])
        except IndexError or AttributeError:
            return 0.0

    def __iter__(self):
        return self

    def __next__(self):
        return self.get_price()


class WorksheetInterface:
    COLUMNS = string.ascii_uppercase
    TIME_SECTORS = ["4:00", "8:00", "12:00", "16:00", "20:00", "24:00"]

    def __init__(self, worksheet_name, spreadsheet, payment_methods):
        self.worksheet_name = worksheet_name
        self.worksheet = spreadsheet.worksheet(self.worksheet_name)
        self.payment_methods = payment_methods
        self.columns_payment = self.COLUMNS[1:len(self.payment_methods) + 1]
        self.prepare_worksheet()

    @staticmethod
    def save_row(row, date):
        with open("last.txt", 'w') as fw:
            fw.write(f"{row},{date}")

    @staticmethod
    def load_row():
        with open("last.txt", 'r') as fr:
            return fr.read().split(',')

    def prepare_worksheet(self):
        self.worksheet.update(f"B1:{self.columns_payment[-1]}1", [self.payment_methods])
        self.worksheet.update("A1", "Limit")
        self.worksheet.update("B3:G3", [self.TIME_SECTORS])
        print(f"Worksheet {self.worksheet_name} prepared.")

    def update_payment_methods(self):
        self.payment_methods = {method: None for method in self.payment_methods}
        for column, (method, value) in enumerate(self.payment_methods.items()):
            self.payment_methods.update({method: self.worksheet.cell(2, column + 1).value})

    def get_limit(self):
        return self.worksheet.acell('A2').value

    def enter_into_cell(self, cell, value):
        self.worksheet.update(cell, value)

    def clear(self):
        self.worksheet.clear()

    def put_current_date(self, row):
        self.enter_into_cell(f"A{row}", datetime.datetime.now().strftime('%d.%m'))
        self.save_row(row, datetime.datetime.now().strftime('%Y-%m-%d').__str__())

    def find_max_spread(self, row):
        columns = ['B', 'C', 'D', 'E', 'F', 'G']
        values = self.worksheet.get_values(f"B{row}:G{row}")[0]
        values = [float(spread.replace(",", ".")) if spread else 0.0 for spread in values]

        columns_dict = (dict(zip(columns, values)))
        return max(columns_dict.items(), key=operator.itemgetter(1))[0]

    def color_biggest_spread(self, row):
        self.worksheet.format(f"{self.find_max_spread(row)}{row}", {
            "backgroundColorStyle": {
                "themeColor": "ACCENT3"}})


class MainRoot:
    def __init__(self, wks_interface: WorksheetInterface):
        self.wks_interface = wks_interface

    def pass_cell(self, cell):
        spot_price = get_spot_price()
        self.wks_interface.update_payment_methods()
        payment_methods = [payment_method for payment_method in self.wks_interface.payment_methods if
                           self.wks_interface.payment_methods[payment_method]]
        limit = self.wks_interface.get_limit()
        if payment_methods:
            spread = round(max(BinanceC2CScraper("RUB", "USDT", limit, "BUY", False, payment_methods)) - spot_price, 3)
        else:
            spread = 0
        self.wks_interface.enter_into_cell(cell, spread)

    def main_loop(self, row, date):
        today = datetime.datetime.now().date()

        while today.__str__() == date.__str__():
            now = datetime.datetime.now()
            self.wks_interface.put_current_date(row)

            today = datetime.datetime.now().strftime('%Y-%m-%d')
            hour = now.hour + now.minute / 60

            timeframes = {4.0: "B", 8.0: "C", 12.0: "D", 16.0: "E", 20.0: "F", 24.0: "G"}
            next_column = min([timeframe for timeframe in timeframes if hour < timeframe], key=lambda x: abs(x - hour))

            print(f"{now.strftime('%d.%m %H:%M:%S')}. Sleeping for {int((next_column - hour) * 3600)} seconds")
            time.sleep((next_column - hour) * 3600)
            self.pass_cell(timeframes[next_column] + row)

        self.wks_interface.color_biggest_spread(row)
        future_date = (date + datetime.timedelta(days=1))
        self.main_loop(str(int(row) + 1), future_date)


def main(spreadsheet_name, filename="service_account.json"):
    payment_methods = ["RosBank", "Tinkoff", "RaiffeisenBankRussia", "QIWI"]
    service_account = gspread.service_account(filename=filename)
    spreadsheet = service_account.open(spreadsheet_name)

    while True:
        worksheet_interface = WorksheetInterface('main', spreadsheet, payment_methods)
        root = MainRoot(worksheet_interface)

        last_row = worksheet_interface.load_row()
        if last_row[0] == '35':
            worksheet_interface.clear()
            continue
        else:
            root.main_loop(last_row[0], datetime.datetime.strptime(last_row[1], '%Y-%m-%d').date())


if __name__ == '__main__':
    main("binance_tracker")
