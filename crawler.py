#!/usr/bin/env python

import requests
import md5
import logging
from queue import Queue, Empty
from multiprocessing.dummy import Pool as ThreadPool 
from urlparse import urlparse
from optparse import OptionParser
from bs4 import BeautifulSoup

# create logger
logger = logging.getLogger('Crawler')
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(asctime)s- %(levelname)s - %(message)s')

MAX_WORKERS = 8
DEPTH = 3

class Cralwer:
    def __init__(self, workers, depth, input_file):
        self.queue = Queue()
        self.pool = ThreadPool(workers)
        self.visited = set([])
        self.input_file = input_file
        self.depth = depth
    
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
        self.pool.apply_async(readFromFile,[self.queue, self.input_file, self.depth])
        while True:
            try:
                url, depth = self.queue.get(timeout=30)
                print url,"xx"
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

def handleOpt(parser):
    nworker = MAX_WORKERS
    depth = DEPTH

    parser.add_option("-f", "--input-file", dest="input_file",
                      help="input image file", action="store", type="string")

    parser.add_option("-d", "--depth", dest="depth",
                      help="depth to crawl", action="store", type="int")

    parser.add_option("-w", "--workers", dest="workers",
                      help="number of workers", action="store", type="int")
    
    (options, args) = parser.parse_args()

    if None == options.input_file:
        parser.error('Error: input file needed')

    if None != options.workers and options.workers > 2:
        nworker = options.workers

    if None != options.depth and options.depth > 0:
        depth = options.depth
    return nworker,depth, options.input_file

def readFromFile(queue, input_file, depth):
    with open(input_file) as fp:
        line = fp.readline()
        while line:
            queue.put([line.rstrip(),depth])
            line = fp.readline()

if __name__ == '__main__':
    parser = OptionParser()
    nworker, depth, input_file = handleOpt(parser)
    app = Cralwer(nworker, depth, input_file)
    logging.debug('Starting Cralwer...')
    app.run()