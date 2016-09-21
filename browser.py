import mechanize
import cookielib


def start_browser(url=None):
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
    if not url:
        br.open('http://denver.craigslist.org')
    else:
        br.open(url)
    print br.title()
    return br
