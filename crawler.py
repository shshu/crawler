import requests
import md5
import logging
from queue import Queue, Empty
from multiprocessing.dummy import Pool as ThreadPool 
from urlparse import urlparse
from bs4 import BeautifulSoup

# create logger with 'spam_application'
logger = logging.getLogger('Crawler')
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(asctime)s- %(levelname)s - %(message)s')

# TODO get number of workers from command line
MAX_WORKERS = 8
DEPTH = 3

class Cralwer:
    def __init__(self, queue):
        self.queue = queue
        self.pool = ThreadPool(MAX_WORKERS)
        self.visited = set([])
    
    def getResponse(self, url):
        try:
            response = requests.get(url)
            return response
        except requests.RequestException:
            return
    
    def doSomthingData(self, reponse):
        pass
    
    def getHrefFromResponse(self, response, url):
        base_url = urlparse(url)
        child_urls = []
        soup = BeautifulSoup(response.content, 'html.parser')
        for a in soup.find_all('a', href=True):
            child_url = a['href']
    
            if urlparse(child_url).scheme == '':
                child_url = '{}://{}{}'.format(base_url.scheme,base_url.netloc,child_url)
            child_urls.append(child_url)
        return child_urls
    
    def crawel(self, url, depth):
        response = self.getResponse(url)
        self.doSomthingData(response)
        if response is None:
            return
        
        if depth == 0:
            return
        
        for child_url in self.getHrefFromResponse(response, url):
            self.queue.put([child_url,depth-1]) 

    def run(self):
        while True:
            try:
                url, depth = self.queue.get(timeout=30)
                md5Url = md5.md5(url).digest()
                if md5Url not in self.visited:
                    logging.debug(' URL: {}'.format(url))
                    self.visited.add(md5Url)
                    self.pool.apply_async(self.crawel,[url,depth])
            except Empty:
                return
            except Exception as e:
                logging.warn(e)
                continue
    
if __name__ == '__main__':
    queue = Queue()
    # TODO get urls from streaming - command line 
    # TODO get depth from command line
    # TODO stream to file the output - command line
    queue.put(['http://www.example.com',DEPTH])
    app = Cralwer(queue)
    logging.debug('Starting Cralwer...')
    app.run()