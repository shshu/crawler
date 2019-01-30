#!/usr/bin/env python2

import md5
import logging
import requests
import argparse
from Queue import Queue, Empty
from multiprocessing.dummy import Pool as ThreadPool 
from urlparse import urlparse
from bs4 import BeautifulSoup

# create logger
logger = logging.getLogger('Crawler')
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(asctime)s- %(levelname)s - %(message)s')

MAX_WORKERS = 8
DEPTH = 3


class URLDepth(object):
    def __init__(self, url, depth):
        self.url = url
        self.depth = depth


class Cralwer:
    def __init__(self, workers, depth, input_file):
        self.queue = Queue()
        self.pool = ThreadPool(workers)
        self.visited = set([])
        self.input_file = input_file
        self.depth = depth
    
    def get_response(self, url):
        try:
            response = requests.get(url)
            return response
        except requests.RequestException:
            return
    
    def do_somthing_data(self, reponse):
        pass
    
    def get_href_from_response(self, response, url):
        base_url = urlparse(url)
        child_urls = []
        soup = BeautifulSoup(response.content, 'html.parser')
        for a in soup.find_all('a', href=True):
            child_url = a['href']
    
            if urlparse(child_url).scheme == '':
                child_url = '{}://{}{}'.format(base_url.scheme,base_url.netloc,child_url)
            child_urls.append(child_url)
        return child_urls
    
    def crawel(self, url_depth):
        url = url_depth.url
        depth = url_depth.depth

        response = self.get_response(url)
        self.do_somthing_data(response)
        if response is None:
            return
        
        if depth == 0:
            return
        
        for child_url in self.get_href_from_response(response, url):
            self.queue.put([child_url, depth-1])

    def get_working_list(self):
        working_list = list()
        while True:
            try:
                url, depth = self.queue.get(timeout=5)
                md5Url = md5.md5(url).digest()
                if md5Url not in self.visited:
                    logging.debug(' URL: {}'.format(url))
                    self.visited.add(md5Url)
                    working_list.append(URLDepth(url, depth))
            except Empty:
                return working_list

    def run(self):
        self.pool.apply_async(read_from_file, [self.queue, self.input_file, self.depth])
        while True:
            try:
                working_list = self.get_working_list()

                if len(working_list) == 0:
                    return

                self.pool.map(self.crawel, working_list)

            except Exception as e:
                logging.warn(e)
                continue


def handle_opt():
    nworker = MAX_WORKERS
    depth = DEPTH

    parser = argparse.ArgumentParser(description='Crawler.')
    parser.add_argument("-f", "--input-file", dest="input_file", help="input image file")
    parser.add_argument("-d", "--depth", dest="depth", help="depth to crawl",action="store", type=int)
    parser.add_argument("-w", "--workers", dest="workers", help="number of workers", action="store", type=int)
    args = parser.parse_args()

    if args.input_file is None:
        parser.error('Error: input file needed')

    if args.workers is not None and args.workers > 2:
        nworker = args.workers

    if args.depth is not None and args.depth > 0:
        depth = args.depth

    return nworker, depth, args.input_file


def read_from_file(queue, input_file, depth):
    with open(input_file) as fp:
        line = fp.readline()
        while line:
            queue.put([line.rstrip(), depth])
            line = fp.readline()


if __name__ == '__main__':
    nworker, depth, input_file = handle_opt()
    app = Cralwer(nworker, depth, input_file)
    logging.debug('Starting Cralwer...')
    app.run()
