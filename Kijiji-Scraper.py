import requests
from bs4 import BeautifulSoup
import ast
import os.path
import datetime
import time
import smtplib
from email.mime.text import MIMEText
import pandas as pd
import csv
import re
import pprint
import configparser
pp = pprint.PrettyPrinter(indent=4)

config = configparser.ConfigParser()
config.read('config.ini')

def clean_string(s):
    s = re.sub(r'[^\x00-\x7F]+', '', s)
    s = re.sub(r'([^\s\w]|_)+', '', s)
    return s

class KijijiScraper():
    def __init__(self):
        self.base_url = "https://www.kijiji.ca"
        self.urls_to_scape = config._sections['urls_to_scape']
        self.scrape_delay = int(config['env']['scrape_delay'])
        self.page_number = 1
        self.exclude_words=['free', 'wanted', 'parts', 'tires', 'brake', 'bumper', 'set','tire','wheel','wheels']

    def ParseAd(self,html):
        ad_info = {}
        ad_info["Title"] = clean_string(html.find_all('a', {"class": "title"})[0].text.strip())
        ad_info["Url"] = 'http://www.kijiji.ca' + html.get("data-vip-url")
        ad_info["Description"] = clean_string(html.find_all('div', {"class": "description"})[0].text.strip())
        tempsoup = html.find_all('div', {"class": "location"})[0].text.strip()
        if tempsoup.find('-') > 0:
            tempsoup = tempsoup[:tempsoup.find('-') - 2]
        ad_info["Location"] = clean_string(tempsoup)
        ad_info["Date"] = clean_string(html.find_all('span', {"class": "date-posted"})[0].text.strip())
        raw_price = html.find_all('div', {"class": "price"})[0].text.strip()
        raw_price_start = raw_price.find('$')
        raw_price_end = raw_price.find('.00')
        ad_info["Price"] = raw_price[raw_price_start:raw_price_end]
        return ad_info

    def write_to_csv(self, desc, new_dict, old_ad_df):
        new_df = pd.DataFrame.from_dict(new_dict, orient='index')
        result = pd.concat([new_df, old_ad_df])
        result.index.name = 'ad_id'
        result.to_csv(desc+'.csv')
    
    def load_ad_db(self, desc):
        if not os.path.isfile(desc+'.csv'):
            return None
        df = pd.read_csv(desc+'.csv', header=0, delimiter=',', index_col='ad_id')
        return df
    
    def MailAd(self, desc, ad_dict):
        if len(ad_dict) < 1:
            print(desc + " No new ads found ")
            return
        import smtplib
        from email.mime.text import MIMEText
        sender = config['env']['sender']
        passwd = config['env']['passwd']
        receivers = config['env']['receivers'].split(',')
        print(sender,passwd,receivers)
        for receiver in receivers:
            count = len(ad_dict)
            if count > 0:
                subject = desc + ' ' + str(count) + ' new ads found'
            else:
                subject = desc + ' No new ads found '
            print(subject)
            body = ''
            try:
                for ad_id in ad_dict:
                    body += ad_dict[ad_id]['Title'] + ' - ' + ad_dict[ad_id]['Price'] + ' - ' + ad_dict[ad_id]['Location']
                    body += ' - ' + ad_dict[ad_id]['Date'] + '\n'
                    body += ad_dict[ad_id]['Url'] + '\n\n'
            except:
                print('[Error] Unable to create body for email message')

            body += 'This is an automated message.\nPlease do not reply to this message.'
            msg = MIMEText(body)
            msg['Subject'] = subject
            msg['From'] = sender
            msg['To'] = receiver

            try:
                server = smtplib.SMTP('smtp.gmail.com:587')
                server.ehlo()
                server.starttls()
            except:
                print('[Error] Unable to connect to email server.')
            try:
                server.login(sender, passwd)
            except:
                print('[Error] Unable to login to email server.')
            try:
                server.sendmail(msg['From'], msg['To'], msg.as_string())
                server.quit()
                print('[Okay] Email message successfully delivered.')
            except:
                print('[Error] Unable to send message.')


    def getNextUrlPage(self,url):
        if(self.page_number == 1):
            self.page_number += 1
            return url
        new_url = ''
        last_slash = url.rfind('/')
        url_prefix = url[:last_slash]
        url_postfix = url[last_slash:]
        new_url = url_prefix+'/page-'+str(self.page_number)+url_postfix
        self.page_number += 1
        return new_url
    
    def scapeContent(self, url):
        try:
            page = requests.get(url)
        except:
            return None
        if(url != page.url):
            return None
        soup = BeautifulSoup(page.content, "html.parser")
        kijiji_ads = soup.find_all("div", {"class": "regular-ad"})
        page = None
        if(len(kijiji_ads) == 0):
            return None
        return kijiji_ads
        
    def run(self):
        for desc, url_to_scrape in self.urls_to_scape.items():
            print(desc)
            self.page_number = 1 #reset page number
            old_ad_df = self.load_ad_db(desc)
            old_ad_id = set()
            if(old_ad_df is not None):
                old_ad_id = set(old_ad_df.index)
                map(int, old_ad_id)
            ad_dict = {}
            try:
                url = self.getNextUrlPage(url_to_scrape)
                kijiji_ads = self.scapeContent(url)
                while(kijiji_ads!=None):
                    for ad in kijiji_ads:
                        try:
                            title = ad.find_all('a', {"class": "title"})[0].text.strip()
                            ad_id = ad.find_all('div', {'class': "watch"})[0].get('data-adid')
                            if not [False for match in self.exclude_words if match in title.lower()] and \
                                int(ad_id) not in old_ad_id:
                                ad_dict[ad_id] = self.ParseAd(ad)
                                old_ad_id.add(ad_id)
                        except:
                            print('error parsing, skip ', url)
                            continue
                    url = self.getNextUrlPage(url_to_scrape)    
                    kijiji_ads = self.scapeContent(url)
            except:
                print("broke")
                pass 
            self.write_to_csv(desc, ad_dict, old_ad_df)
            self.MailAd(desc, ad_dict)

        print("done scrapping everything, sleeping for now")
        time.sleep(self.scrape_delay)
        self.run()

if __name__ == "__main__":
    k = KijijiScraper()
    k.run()
    