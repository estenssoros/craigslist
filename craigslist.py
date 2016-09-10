from __future__ import division
from pymongo import MongoClient
from bs4 import BeautifulSoup
from dateutil import parser
import mechanize
import cookielib
import time
import pandas as pd
import progressbar
import matplotlib.path as mplPath
import numpy as np
import folium


def start_browser(url):
    br = mechanize.Browser(factory=mechanize.RobustFactory())
    cj = cookielib.LWPCookieJar()
    br.set_cookiejar(cj)

    br.set_handle_equiv(True)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.set_handle_robots(False)

    br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

    br.addheaders = [
        ('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
    br.open(url)
    print br.title()
    return br


def get_links(br, k=0):
    print 'page: {0}'.format(k)
    links = []
    if 'no results' in br.response().read():
        return links
    for link in br.links():
        if len(link.attrs) == 3 and link.attrs[1][0] == 'data-id':
            links.append(link)
        if link.text == 'next >' and len(links) > 0:
            print len(links)
            time.sleep(0.5)
            br.follow_link(link)
            links.extend(get_links(br, k=k + 1))
            return links


def parse_html(html):
    soup = BeautifulSoup(html, 'html')
    maps = soup.findAll('div', {'id': 'map'})
    if len(maps) == 1:
        lat = maps[0].attrs['data-latitude']
        lon = maps[0].attrs['data-longitude']

    elif len(maps) > 1:
        raise ValueError('more than one mapbox?')
    else:
        pass
    price = soup.find('span',{'class':'price'}).text
    housing = soup.find('span',{'class':'housing'}).text


def insert_mongo(br, links):
    client = MongoClient()
    client.drop_database('craigslist')

    db = client['craigslist']
    coll = db['housing']

    bar = progressbar.ProgressBar()
    for link in bar(links):
        br.follow_link(link)
        html = br.response().read()
        dic = {'_id': link.attrs[1][1], 'html': html}
        coll.insert_one(dic)


def draw_map():
    p1 = (39.702248, -104.987396)
    p2 = (39.691106, -104.974581)
    p3 = (39.691106, -104.959347)
    p4 = (39.738317, -104.959347)
    p5 = (39.738317, -104.940903)
    p6 = (39.758320, -104.940903)
    p7 = (39.758320, -104.973331)
    p8 = (39.771182, -104.973331)
    p9 = (39.743186, -105.014430)
    p10 = (39.718450, -105.003763)
    points = [p1, p2, p3, p5, p6, p7, p8, p9, p10, p1]

    ave_lat = sum(p[0] for p in points) / len(points)
    ave_lon = sum(p[1] for p in points) / len(points)

    my_map = folium.Map(location=[ave_lat, ave_lon], tiles='Stamen Toner', zoom_start=13)
    for i, each in enumerate(points):
        popup = '{0} - {1}'.format(i + 1, each)
        folium.RegularPolygonMarker(each, popup=popup, fill_color='#ff0000', number_of_sides=8, radius=4).add_to(my_map)
    #     folium.Marker(each, popup=popup).add_to(my_map)
    folium.PolyLine(points, color="red", weight=2.5, opacity=1).add_to(my_map)
    my_map.save("./housing_boundaries.html")


def make_df(links):
    df = pd.DataFrame()
    bar = progressbar.ProgressBar()
    for link in bar(links):
        df = df.append({'id': link.attrs[1][1]}, ignore_index=True)
    df['link'] = 'http://denver.craigslist.org/apa/' + df['id'] + '.html'
    return df


def main():
    url = "http://denver.craigslist.org/search/apa?hasPic=1&search_distance=3&postal=80206&max_price=2000&pets_dog=1"
    br = start_browser(url)
    links = get_links(br)
    # df = make_df(links)
    insert_mongo(br, links)

if __name__ == '__main__':
    # draw_map()
    # main()
    pass
