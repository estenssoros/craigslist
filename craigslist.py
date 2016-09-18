from __future__ import division
from pymongo import MongoClient
import mechanize
import cookielib
import time
import pandas as pd
from progressbar import Percentage, Bar, ProgressBar, Counter, ETA
import matplotlib.path as mplPath
import numpy as np
import folium
import subprocess
import datetime as dt
from parsers import *
from aws import *


class CraigsList(object):

    def __init__(self, url):
        self.url = url
        self.br = self.start_browser()
        self.coll = self.connect_mongo()
        self.points = self.get_boundary_points()

    def start_browser(self):
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
        br.open(self.url)
        print br.title()
        return br

    def get_links(self, k=0):
        print 'page: {0}'.format(k)
        links = []
        if 'no results' in self.br.response().read():
            return links
        for link in self.br.links():
            if len(link.attrs) == 3 and link.attrs[1][0] == 'data-id':
                links.append(link)
            if link.text == 'next >' and len(links) > 0:
                print len(links)
                time.sleep(0.5)
                self.br.follow_link(link)
                links.extend(self.get_links(k=k + 1))
                return links

    def connect_mongo(self):
        client = MongoClient()
        db = client['craigslist']
        coll = db['housing']
        return coll

    def insert_mongo(self):
        old_ids = [x['_id'] for x in self.coll.find()]
        today_ids = [link.attrs[1][1] for link in self.links]

        new_ids = [x for x in today_ids if x not in old_ids]
        delete_ids = [x for x in old_ids if x not in today_ids]

        for id in delete_ids:
            self.coll.delete_one({'_id': id})

        self.new_links = [link for link in self.links if link.attrs[1][1] in new_ids]

        if len(self.new_links) > 0:
            bar = ProgressBar(widgets=['Fetching: ', Counter(), '/{} '.format(len(self.new_links)), Bar(), ' ', ETA()])
            for link in bar(self.new_links):
                time.sleep(0.5)
                try:
                    self.br.follow_link(link)
                    html = self.br.response().read()
                    dic = parse_html(html)
                    dic.update({'_id': link.attrs[1][1]})
                    self.coll.insert_one(dic)
                except Exception as e:
                    self.coll.insert_one({'_id': link.attrs[1][1]})
                    self.br = self.start_browser()
                    with open('error_log.txt', 'a') as f:
                        f.write('{0} - {1} \n'.format(e, link.absolute_url))
                        subprocess.Popen(['open', link.absolute_url])

    def get_boundary_points(self):
        p1 = (39.702248, -104.987396)
        p2 = (39.691106, -104.974581)
        p3 = (39.691106, -104.959347)
        p4 = (39.718136, -104.959347)
        p5 = (39.718136, -104.940903)
        p6 = (39.758320, -104.940903)
        p7 = (39.758320, -104.973331)
        p8 = (39.771182, -104.973331)
        p9 = (39.743186, -105.014430)
        p10 = (39.718450, -105.003763)
        points = [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p1]
        return points

    def make_df(self):
        print 'Building DataFrame...'
        df = pd.DataFrame()
        for r in self.coll.find({'attributes' : {'$exists' : True}}):
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

        yesterday = dt.datetime.today().date() - dt.timedelta(days=1)

        m = df['updated'].isnull()
        df.loc[m, 'updated'] = df.loc[m, 'posted']

        df['new'] = False
        m = df['updated'] >= yesterday
        df.loc[m, 'new'] = True

        print 'DataFrame constructed!'
        return df

    def plot_coord(self):

        bbPath = mplPath.Path(np.array(self.points))

        df = self.df[self.df.apply(lambda x: bbPath.contains_point((x['lat'], x['lon'])), axis=1)]
        df = df[df['apartment'] == 'No']
        df.reset_index(drop=True, inplace=True)
        my_map = folium.Map(location=[df['lat'].mean(), df['lon'].mean()], tiles='Stamen Toner', zoom_start=13)

        pbar = ProgressBar(widgets=['Plotting: ', Counter(),
                                    '/{0} '.format(len(df) + 1), Bar(), ' ', ETA()], maxval=len(df) + 1).start()
        self.new = [link for link in self.new_links if link in df['id'][df['new'] == True].values.tolist()]

        for i, r in df.iterrows():

            if r['new'] == True:
                fill_color = '#33cc33'
            else:
                fill_color = '#0000ff'

            html = '{1}br/{2}ba | {3} ft2 | ${4} <br> <a href="http://denver.craigslist.org/apa/{0}.html", target="_blank">link</a> '.format(r[
                                                                                                                                             'id'], r['bed'], r['bath'], r['sqft'], r['price'])
            iframe = folium.element.IFrame(html=html, width=200, height=50)
            poppin = folium.Popup(html=iframe)
            folium.RegularPolygonMarker((r['lat'], r['lon']),
                                        popup=poppin,
                                        fill_color=fill_color,
                                        number_of_sides=8,
                                        radius=6).add_to(my_map)
            pbar.update(i + 1)
        pbar.finish()

        for i, each in enumerate(self.points):
            popup = '{0} - {1}'.format(i + 1, each)
            folium.RegularPolygonMarker(each, popup=popup, fill_color='#ff0000', number_of_sides=3, radius=6).add_to(my_map)
        folium.PolyLine(self.points, color="red", weight=2.5, opacity=1).add_to(my_map)

        my_map.save("./found.html")
        subprocess.Popen(['open', 'found.html'])
        print 'plotting done!'

    def upload_s3(self):
        bucket = connect_s3()
        key = bucket.new_key('craigslist/found.html')
        key.set_contents_from_filename('found.html')
        if len(self.new) > 0:
            message = "{0} new entries uploaded to http://www.estenssoros.com/craigslist/found.html".format(len(self.new))
            send_message(message)

    def run(self):
        self.links = self.get_links()
        self.insert_mongo()
        self.df = self.make_df()
        self.plot_coord()
        self.upload_s3()


def main():
    url = "http://denver.craigslist.org/search/apa?hasPic=1&search_distance=3&postal=80206&max_price=2000&pets_dog=1"
    craigslist = CraigsList(url)
    craigslist.run()

if __name__ == '__main__':
    main()
    # bucket = connect_s3()
