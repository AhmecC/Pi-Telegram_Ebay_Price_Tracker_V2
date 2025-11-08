import subprocess, logging, sys, telebot, time, sqlite3, re, pandas as pd, numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from datetime import datetime as dt
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(message)s", datefmt="%H:%M:%S")

 # --- Configure Bot
bot = telebot.TeleBot('')

# --- Configure Database
db = sqlite3.connect('', check_same_thread=False)
cur = db.cursor()










class Scraper:
    def __init__(self, item, id):
        self.START = time.time()
        self.END = 0
        self.item = item.replace(' ', '-')
        self.numResults = None
        self.df = None
        self.userID = id
        self.userItem = item

    def driver_configure(self):  # <--- Driver configured on call to ensure driver independence
        options = Options()
        options.add_argument('--headless')
        service = Service(executable_path='/usr/local/bin//geckodriver')
        return options, service    
    
    def total_price(self, x):
        """Retrieves Item/Postage Price and returns total"""
        prices = [float(i) for i in re.findall(r'£(\d+\.+\d*)', x)]
        return np.round(sum(prices), 2)
    
    def convert_into_hours(self, x):
        """If exists, returns time left in minutes"""
        minutes = 0
        try:
            x = x.split('\n')
            timeLeft = x[x.index('Time left') +1]
            for i in ['d', 'h', 'm']:
                a = re.findall(f'(\d+){i}', timeLeft)  # Looks for numbers before d,h,m
                if i == 'd' and a:
                    minutes += int(a[0]) * 24 * 60
                if i == 'h' and a:
                    minutes += int(a[0]) * 60
                if i == 'm' and a:
                    minutes += int(a[0])
            return minutes 
        except ValueError:  # Only interested where Time left exists
            return None




 
    def item_Scraper(self, glance, dbIngest):
        """Scrape Item and retrieve Results, Metadata and Links"""
        try:
            o, s = self.driver_configure()
            with webdriver.Firefox(options=o, service=s) as driver:
                # --- Use driver to search item
                driver.get('https://www.ebay.co.uk/sch/i.html?_nkw={}'.format(self.item))
                time.sleep(5)
                logging.info(f'Succesfully searched: {self.item}')

                # --- Retrieve number of relevant results
                nums = (driver.find_elements(By.CLASS_NAME, 'srp-controls__count-heading'))[0].text
                self.numResults = int("".join(re.findall(r'\d+', nums)))

                # --- Retrieve Metadata/Links information.
                data = [s.text for s in driver.find_elements(By.CLASS_NAME, 'su-card-container__content')]  # Item details
                links = [s.get_attribute('href') for s in driver.find_elements(By.CLASS_NAME, 's-card__link')][::2]  # Item hyperlinks
                logging.info(f'Metadata Retrieved [Results: {self.numResults} | Data: {len(data)} | Links: {len(links)}]')

                if glance:  # --- glance allows for Quick Top 3 without adding to database
                    time.sleep(5)
                    self.item_manipulation(data, links, dbIngest)      
        except Exception:
            if not dbIngest:  # -- Don't send during crontab run
                bot.send_message(self.userID, f'Sorry an Error has ocurred whilst Scraping. Please Try Again!')
            logging.exception(f'Scrape Error has Ocurred')
        finally:
            self.END = time.time() - self.START
            subprocess.run(['pkill', '-f', 'geckodriver'])  # < --- Ensures driver independence
            subprocess.run(['pkill', '-f', 'firefox'])
        return self




 
    def item_manipulation(self, data, links, dbIngest):
        """Clean and Manipulate Search Results"""
        try:
            df = pd.DataFrame({'Metadata':data, 'Hyperlink':links})
            df['Type'] = np.select([df['Metadata'].str.lower().str.contains('buy it now'), df['Metadata'].str.lower().str.contains('time left'), df['Metadata'].str.lower().str.contains('best offer')], ['buy it now', 'auction', 'buy it now'], None)
            df['Item_ID'] = df['Hyperlink'].str.extract(r'itm/(\d+)').astype('Int64').astype('object').where(lambda s: pd.notnull(s), None)
            df['Name'] = df['Metadata'].apply(lambda x: x.split('\n')[0] if x else None)
            df['Minutes'] = df['Metadata'].apply(self.convert_into_hours)
            df['Price'] = df['Metadata'].apply(self.total_price)
            df['shortLink'] = df['Item_ID'].apply(lambda x: 'https://www.ebay.co.uk/itm/{}'.format(x))
            df['Status'] = 1
            df['userID'] = self.userID
            df['userItem'] = self.userItem
            df.dropna(subset=['Metadata', 'Hyperlink', 'Item_ID', 'Name', 'shortLink'], inplace=True)  
            self.df = df[2:(2+self.numResults)] if self.numResults < len(df) else df[2:]  # If results<df, remove extra as likely irrelevant
            logging.info(f'Metadata Manipulation Complete')
            
            if dbIngest:  # -- dbIngest allows for results ingestion into database
               self.ingestion()
        except Exception:
            if not dbIngest:  # -- Don't send during crontab run
                bot.send_message(self.userID, f'Sorry an Error has ocurred during Preperation. Please Try Again!')
            logging.exception(f'Manipulation Error has Ocurred')
        finally:
            self.END = time.time() - self.START




 
    def ingestion(self):
        try:
            cur.execute("UPDATE TRACKED_LIST SET Status=0 WHERE userID=? AND userItem=?", (self.userID, self.userItem))
            db.commit()  # -- Set existing Status=0, as they'll be overwritten for stillActive
            toIngest = [tuple(row) for row in self.df[['userID','userItem','Name','Item_ID','Type','Minutes','Price','Hyperlink','shortLink','Metadata','Status']].values]
            cur.executemany("INSERT OR REPLACE INTO TRACKED_LIST (userID, userItem, Name, Item_ID, Type, Minutes, Price, Hyperlink, shortLink, Metadata, Status) VALUES (?,?,?,?,?,?,?,?,?,?,?)", toIngest)
            db.commit()  # --- Batch insert all newlyTracked
            logging.info(f'Data Ingestion complete')

        except Exception:
            logging.exception(f'Ingestion Error has Ocurred')


















##### -------------------------------------------------- CRONJOB LOGIC (Run this File at Specified Times) -------------------------------------------------- #####

def toSend_formatter(toSend):
    """Special characters must be backslashed to be sent"""
    toSend = str(toSend)
    for i in "\_*[],()~>#+-_=|!.'":
        toSend = toSend.replace(i, f"\\{i}")
    return toSend





if __name__ == "__main__":
    def itemScraper(item):
        scraper = Scraper(item[1], item[0])
        scraper.item_Scraper(glance=True, dbIngest=True)
        df = scraper.df
        candidates = df[((df.Price < int(item[2])) & (df.Price >= int(item[2])*0.5)) & ((df.Type == 'Auction') & (df.Minutes < 600) | (df.Type != 'Auction'))].sort_values('Type', ascending=False)[:3]
        return candidates  # --- Candidate Logic: <£tgt and >0.5*£tgt, <600mins for Auction
    
    # --- toScrape_IDs returned as ID: [All Applicable Items]
    response = sys.argv[1]
    vals, toScrape_IDs = {'Type1':[3], 'Type2':[0,1,2], 'Type3':[3], 'Type4':[2], 'Type5':[3]}.get(response), {}
    toScrape = cur.execute(f"SELECT * FROM TRACKED_ITEMS WHERE Frequency IN ({','.join('?'*len(vals))}) ORDER BY ID", vals).fetchall()
    for row in toScrape:
        if row[0] in toScrape_IDs.keys():
            toScrape_IDs[row[0]].append(row)
        else:
            toScrape_IDs[row[0]] = [row]

    # --- Loop through all user Items then send together
    for userID, userItems in toScrape_IDs.items():
        allToSend = []
        for item in userItems:
            message = f"Matches for {item[1].replace('-', ' ')} \[Target: £{toSend_formatter(item[2])}\]\n\n"
            try:
                matched = itemScraper(item)
                if len(matched) > 0:
                    for i, row in matched.iterrows():
                        toSend = '\- {} Item found for [£{}]({})\n\n'.format((row.Type).capitalize(), toSend_formatter(row.Price), toSend_formatter(row.shortLink)) if row.Type != 'Auction' \
                        else '\- Auction Item found with {} minutes left is currently [£{}]({})\n\n'.format(toSend_formatter(int(row.Minutes)), toSend_formatter(row.Price), toSend_formatter(row.shortLink))
                        message += toSend
                else:
                    message = f"😔 No Satisfactory matches were found for '{item[1]}'!"
            except Exception as e:
                logging.exception(f'Error ocurred during Send Off Call')
                message = f"😔 An Error Ocurred whilst matching for '{item[1]}'!"
            allToSend.append(message)
        
        for i in allToSend:
            bot.send_message(userID, i, parse_mode='MarkdownV2')
