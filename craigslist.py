from __future__ import division
from pymongo import MongoClient
from bs4 import BeautifulSoup
from dateutil import parser
import mechanize
import cookielib
import time
import pandas as pd
from progressbar import Percentage, Bar, ProgressBar, Counter
import matplotlib.path as mplPath
import numpy as np
import folium
import re


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
    lat = None
    lon = None
    price = None
    housing = None
    info = None
    posted = None
    updated = None

    soup = BeautifulSoup(html, 'html')

    maps = soup.findAll('div', {'id': 'map'})
    if len(maps) == 1:
        lat = maps[0].attrs['data-latitude']
        lon = maps[0].attrs['data-longitude']
    elif len(maps) > 1:
        raise ValueError('more than one mapbox?')
    price = soup.find('span', {'class': 'price'}).text

    info = soup.findAll('p', {'class': 'postinginfo reveal'})
    for i in info:
        if 'posted' in i.text.lower():
            posted = i.text
        elif 'updated' in i.text.lower():
            updated = i.text

    attrs_group = []
    attrs = soup.findAll('p', {'class': 'attrgroup'})
    for a in attrs:
        spans = a.findAll('span')
        for s in spans:
            attrs_group.append(s.text)
    dic = {'coord': (lat, lon), 'price': (price,), 'info': (posted, updated), 'attributes': tuple(attrs_group)}
    return dic


def insert_mongo(br, links):
    client = MongoClient()
    client.drop_database('craigslist')

    db = client['craigslist']
    coll = db['housing']

    bar = ProgressBar()
    for link in bar(links):
        time.sleep(0.5)
        try:
            br.follow_link(link)
            html = br.response().read()
            dic = parse_html(html)
            dic.update({'_id': link.attrs[1][1]})
            coll.insert_one(dic)
        except Exception as e:
            br = start_browser('http://denver.craigslist.org/')
            with open('error_log.txt', 'a') as f:
                f.write(str(e) + '\n')


def get_boundary_points():
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
    points = [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p1]
    return points


def draw_map():
    points = get_boundary_points()
    ave_lat = sum(p[0] for p in points) / len(points)
    ave_lon = sum(p[1] for p in points) / len(points)

    my_map = folium.Map(location=[ave_lat, ave_lon], tiles='Stamen Toner', zoom_start=13)
    for i, each in enumerate(points):
        popup = '{0} - {1}'.format(i + 1, each)
        folium.RegularPolygonMarker(each, popup=popup, fill_color='#ff0000', number_of_sides=8, radius=4).add_to(my_map)
    #     folium.Marker(each, popup=popup).add_to(my_map)
    folium.PolyLine(points, color="red", weight=2.5, opacity=1).add_to(my_map)
    my_map.save("./housing_boundaries.html")


def parse_attrs(attrs):
    bed = None
    bath = None
    sqft = None
    available = None
    laundry = None
    apartment = 'No'
    attrs = [a.lower() for a in attrs]
    for a in attrs:
        if '/' in a and 'br' in a:
            bed = re.findall('([0-9a-z]+)br', a)[0]
            bath = re.findall('([0-9.a-z]+)ba', a)[0]
        elif 'ft2' in a:
            sqft = int(a.split('ft2')[0])
        elif 'available' in a:
            available = a
        elif 'laundry' in a:
            laundry = a
        elif 'apartment' in a:
            apartment = 'Yes'

    return bed, bath, sqft, available, laundry, apartment


def parse_info(info):
    posted, updated = info
    posted = parser.parse(posted.split('ed:')[1]).date()
    if updated:
        updated = parser.parse(updated.split('ed:')[1]).date()
    return posted, updated


def make_df():
    client = MongoClient()
    db = client['craigslist']
    coll = db['housing']
    df = pd.DataFrame()
    for r in coll.find():
        attrs = r['attributes']
        bed, bath, sqft, available, laundry, apartment = parse_attrs(attrs)
        posted, updated = parse_info(r['info'])
        df = df.append({'id': r['_id'],
                        'lat': r['coord'][0],
                        'lon': r['coord'][1],
                        'price': r['price'][0],
                        'bed': bed,
                        'bath': bath,
                        'sqft': sqft,
                        'available': available,
                        'laundry': laundry,
                        'apartment': apartment,
                        'posted': posted,
                        'updated': updated}, ignore_index=True)
    df = df[df['lat'].notnull()]
    df['price'] = df['price'].str.replace('$', '').astype(int)
    cols = ['lat', 'lon']
    for col in cols:
        df[col] = df[col].astype(float)

    print 'df constructed!'
    return df

def plot_coord(df):
    points = get_boundary_points()
    bbPath = mplPath.Path(np.array(points))

    df = df[df.apply(lambda x: bbPath.contains_point((x['lat'], x['lon'])), axis=1)]
    df = df[df['apartment'] =='No']
    df.reset_index(drop=True, inplace=True)
    my_map = folium.Map(location=[df['lat'].mean(), df['lon'].mean()], tiles='Stamen Toner', zoom_start=13)

    pbar = ProgressBar(widgets=['Plotting: ', Counter(), Bar()], maxval=len(df) + 1).start()
    for i, r in df.iterrows():
        popup = 'http://denver.craigslist.org/apa/{0}.html'.format(r['id'])
        folium.RegularPolygonMarker((r['lat'], r['lon']),
                                    popup=popup,
                                    fill_color='#0000ff',
                                    number_of_sides=8,
                                    radius=4).add_to(my_map)

        pbar.update(i + 1)
    pbar.finish()
    for i, each in enumerate(points):
        popup = '{0} - {1}'.format(i + 1, each)
        folium.RegularPolygonMarker(each, popup=popup, fill_color='#ff0000', number_of_sides=3, radius=4).add_to(my_map)
    folium.PolyLine(points, color="red", weight=2.5, opacity=1).add_to(my_map)
    my_map.save("./found.html")
    print 'plotting done!'


def fetch_day():
    url = "http://denver.craigslist.org/search/apa?hasPic=1&search_distance=3&postal=80206&max_price=2000&pets_dog=1"
    br = start_browser(url)
    links = get_links(br)
    insert_mongo(br, links)

if __name__ == '__main__':
    df = make_df()
    plot_coord(df)
