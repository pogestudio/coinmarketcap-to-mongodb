# coding: utf-8

from lxml import html
import requests
import pandas as pd
import config

# DATABASE THINGS
# this one is used by the database
import pymongo as pm

# connect to the database choosing the correct collection
mongo_db_url = config.mongo_db_url
mongo_client = pm.MongoClient(mongo_db_url)
db = mongo_client[config.database_name]  # the selected database on your mongo server
collection = db[config.collection_name]  # the collection to which to write the data

baseURL = 'https://coinmarketcap.com/historical/';

snapshotDates = ['20130505', '20130512', '20130519', '20130526', '20130602', '20130609', '20130616', '20130623', '20130630', '20130707', '20130714', '20130721', '20130728', '20130804', '20130811',
    '20130818', '20130825', '20130901', '20130908', '20130915', '20130922', '20130929', '20131006', '20131013', '20131020', '20131027', '20131103', '20131110', '20131117', '20131124', '20131201',
    '20131208', '20131215', '20131222', '20131229', '20140105', '20140112', '20140119', '20140126', '20140202', '20140209', '20140216', '20140223', '20140302', '20140309', '20140316', '20140323',
    '20140330', '20140406', '20140413', '20140420', '20140427', '20140504', '20140511', '20140518', '20140525', '20140601', '20140608', '20140615', '20140622', '20140629', '20140706', '20140713',
    '20140720', '20140727', '20140803', '20140810', '20140817', '20140824', '20140831', '20140907', '20140914', '20140921', '20140928', '20141005', '20141012', '20141019', '20141026', '20141102',
    '20141109', '20141116', '20141123', '20141130', '20141207', '20141214', '20141221', '20141228', '20150104', '20150111', '20150118', '20150125', '20150201', '20150208', '20150215', '20150222',
    '20150301', '20150308', '20150315', '20150322', '20150329', '20150405', '20150412', '20150419', '20150426', '20150503', '20150510', '20150517', '20150524', '20150531', '20150607', '20150614',
    '20150621', '20150628', '20150705', '20150712', '20150719', '20150726', '20150802', '20150809', '20150816', '20150823', '20150830', '20150906', '20150913', '20150920', '20150927', '20151004',
    '20151011', '20151018', '20151025', '20151101', '20151108', '20151115', '20151122', '20151129', '20151206', '20151213', '20151220', '20151227', '20160103', '20160110', '20160117', '20160124',
    '20160131', '20160207', '20160214', '20160221', '20160228', '20160306', '20160313', '20160320', '20160327', '20160403', '20160410', '20160417', '20160424', '20160501', '20160508', '20160515',
    '20160522', '20160529', '20160605', '20160612', '20160619', '20160626', '20160703', '20160710', '20160717', '20160724', '20160731', '20160807', '20160814', '20160821', '20160828', '20160904',
    '20160911', '20160918', '20160925', '20161002', '20161009', '20161016', '20161023', '20161030', '20161106', '20161113', '20161120', '20161127', '20161204', '20161211', '20161218', '20161225',
    '20170101', '20170108', '20170115', '20170122', '20170129', '20170205', '20170212', '20170219', '20170226', '20170305', '20170312', '20170319', '20170326', '20170402', '20170409', '20170416',
    '20170423', '20170430', '20170507', '20170514', '20170521', '20170528', '20170604', '20170611', '20170618', '20170625', '20170702', '20170709', '20170716', '20170723', '20170730', '20170806',
    '20170813', '20170820', '20170827', '20170903', '20170910', '20170917', '20170924', '20171001', '20171008', '20171015', '20171022', '20171029', '20171105', '20171112', '20171119', '20171126',
    '20171203', '20171210', '20171217', '20171224', '20171231', '20180107', '20180114', '20180121', '20180128', '20180204', '20180211', '20180218', '20180225', '20180304', '20180311', '20180318',
    '20180325', '20180401', '20180408', '20180415', '20180422', '20180429', '20180506', '20180513', '20180520', '20180527', '20180603', '20180610', '20180617', '20180624', '20180701', '20180708',
    '20180715', '20180722', '20180729', '20180805', '20180812', '20180819', '20180826', '20180902', '20180909', '20180916', '20180923', '20180930', '20181007']

start_amount = len(snapshotDates)


def maybe_float( text ):
    try:
        if isinstance(text, pd.Series):
            return float(text.tolist()[0])
        return float(text)
    except (ValueError, IndexError):
        return 0

def parse_snapshot( date ):
    fullURL = baseURL + date + '/';
    if config.DEBUG:
        print("starting URL parsing snapshot for: " + date);
    resp = requests.get(fullURL)
    h = html.fromstring(resp.content)
    names = h.xpath('//a[@class="currency-name-container link-secondary"]/text()')
    symbols = h.xpath('//td[@class="text-left col-symbol"]/text()')
    symbols = [replaceSymbolCharacters(symbol) for symbol in symbols];
    market_caps = [maybe_float(row) for row in h.xpath('//td[@class="no-wrap market-cap text-right"]/@data-usd')]
    oneday_volumes = [maybe_float(row) for row in h.xpath('//a[@class="volume"]/@data-usd')]
    prices_usd = [maybe_float(row) for row in h.xpath('//a[@class="price"]/@data-usd')]
    prices_btc = [maybe_float(row) for row in h.xpath('//a[@class="price"]/@data-btc')]
    formattedForReturn = {};
    for x in range(0, len(symbols)):
        formattedForReturn[symbols[x]] = {'name': names[x], 'symbol': symbols[x], 'market_cap': market_caps[x], 'oneday_volume': oneday_volumes[x], 'price_usd': prices_usd[x],
                                          'price_btc': prices_btc[x]};
    if config.DEBUG:
        print("Finished parsing " + date);
    return formattedForReturn

def write_snapshotresults_to_database( datesAndData ):
    result = collection.insert_many(datesAndData)
    #print("wrote " + str(len(datesAndData)) + " to db!");
    result.inserted_ids

def replaceSymbolCharacters( stringToFix ):
    symbols_that_does_not_work_in_mongo_and_their_replacements = {'$': 'SSS'};
    for symbol, replacement in symbols_that_does_not_work_in_mongo_and_their_replacements.items():
        #         print("want to replace" + symbol + " in string " + stringToFix + " with " + replacement);
        stringToFix = stringToFix.replace(symbol, replacement);
    return stringToFix;

def parse_and_save_data( snapshotDatesToParse ):
    while len(snapshotDatesToParse) > 0:
        # first parse
        parsedSnapshots = [];
        limit = 2;
        counter = 0;
        while counter < limit and len(snapshotDatesToParse) > 0:
            snapshotDate = snapshotDatesToParse.pop();
            entry = {};
            entry['date'] = snapshotDate;
            entry['marketData'] = parse_snapshot(snapshotDate);
            parsedSnapshots.append(entry);
            #             print(parsedSnapshots);
            counter += 1;
        # then save
        write_snapshotresults_to_database(parsedSnapshots)
        progress_number = float(start_amount - len(snapshotDatesToParse)) / float( start_amount) * 100
        progress_string = "{:.1f}".format(progress_number) + "%"


        print("wrote to database, progress: " + progress_string)


parse_and_save_data(snapshotDates);  # write_snapshotresults_to_database(allRecordedSnapshots);