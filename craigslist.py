from __future__ import division
from pymongo import MongoClient
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
from browser import start_browser
import os


class CraigsList(object):

    def __init__(self, url):
        self.url = url
        self.br = start_browser(self.url)
        self.coll = self.connect_mongo()
        self.points = self.get_boundary_points()

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
            bar = ProgressBar(widgets=['Fetching: ', Counter(), '/{} |'.format(len(self.new_links)), Percentage(),' ',Bar(), ' ', ETA()])
            for link in bar(self.new_links):
                time.sleep(0.5)
                try:
                    self.br.follow_link(link)
                    html = self.br.response().read()
                    dic = parse_html(html)
                    dic.update({'_id': link.attrs[1][1]})
                    self.coll.insert_one(dic)
                except Exception as e:
                    try:
                        self.coll.insert_one({'_id': link.attrs[1][1]})
                    except:
                        pass
                    self.br = start_browser(self.url)
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
        p9 = (39.754689, -104.994373)
        p10 = (39.761634, -105.004886)
        p11 = (39.767492, -104.997945)
        p12 = (39.783419, -104.998255)
        p13 = (39.783419, -105.052862)
        p14 = (39.740712, -105.053053)
        p15 = (39.740758, -105.012972)
        points = [p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p1]
        return points

    def make_df(self):
        print 'Building DataFrame...'
        df = pd.DataFrame()
        for r in self.coll.find({'attributes': {'$exists': True}}):
            attrs = r['attributes']
            bed, bath, sqft, available, laundry, apartment = parse_attrs(attrs)
            posted, updated = parse_info(r['info'])
            df = df.append({'id': r['_id'],
                            'image': r['image'],
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

        bbPath = mplPath.Path(np.array(self.points))

        df = df[df.apply(lambda x: bbPath.contains_point((x['lat'], x['lon'])), axis=1)]
        df = df[df['apartment'] == 'No']
        df.reset_index(drop=True, inplace=True)

        lst = df['id'][df['new'] == True].values.tolist()
        links = [link.attrs[1][1] for link in self.new_links]
        self.new = [link for link in links if link in lst]

        return df

    def get_images(self):
        downloaded = [x.split('.jpg')[0] for x in os.listdir('images/') if x.endswith('.jpg')]
        needs = [id for id in self.df['id'] if id not in downloaded]
        urls = {id: list(self.df.loc[self.df['id'] == id, 'image'])[0] for id in needs}
        self.br = start_browser()
        for id, url in urls.iteritems():
            data = self.br.open(url).read()
            with open('images/{0}.jpg'.format(id), 'wb') as f:
                f.write(data)
            self.br.back()
            time.sleep(0.5)

    def plot_coord(self):
        my_map = folium.Map(location=[self.df['lat'].mean(), self.df['lon'].mean()], tiles='Stamen Toner', zoom_start=12)
        pbar = ProgressBar(widgets=['Plotting: ', Counter(),
                                    '/{0} '.format(len(self.df) + 1), Bar(), ' ', ETA()], maxval=len(self.df) + 1).start()

        for i, r in self.df.iterrows():

            if r['new'] == True:
                fill_color = '#33cc33'
            else:
                fill_color = '#0000ff'

            html = '''<html>
            <body>
            {1}br/{2}ba | {3} ft2 | ${4} |
            <a href="http://denver.craigslist.org/apa/{0}.html", target="_blank">link</a>
            <br>
            <img src="{5}" style="width:300">
            </body>
            </html>'''.format(r['id'], r['bed'], r['bath'], r['sqft'], r['price'], r['image'])
            # iframe = folium.element.IFrame(html=html, width=250, height=50)
            iframe = folium.element.IFrame(html=html, width=300, height=250)
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
    for i in range(4):
        url = "http://denver.craigslist.org/search/apa?hasPic=1&search_distance=5&postal=80206&max_price=2000&pets_dog=1"
        craigslist = CraigsList(url)
        craigslist.run()
        bar = ProgressBar(widgets=['Waiting: ', Counter(), '/3600 |', Percentage(),' ',Bar(), ' ', ETA()])
        for t in bar(range(3600)):
            time.sleep(1)   

if __name__ == '__main__':
    c = main()
