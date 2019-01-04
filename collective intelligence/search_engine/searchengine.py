import urllib.request
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from sqlite3 import dbapi2 as sqlite
import re





#Function to make the different scoring methods return a score from 0 to 1
# where higher means better and 1 means best result
def normalize_scores( scores, small_is_better = False):
    small_number = 0.000001 # to avoid division by 0 error
    if small_is_better:
        minscore = min(scores.values())
        return dict([(u, float(minscore) / max(small_number, l)) for (u, l) \
                 in scores.items()])
    else:
        maxscore = max(scores.values())
        if maxscore == 0: maxscore = small_number
        return dict([(u, float(c) / maxscore) for (u, c) in scores.items()])




# Create a list of words to ignore
ignore_words = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])

class crawler:


    # Initialize the crawler with the name of database
    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    def createindextables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid,wordid,location)')
        self.con.execute('create table link(fromid integer,toid integer)')
        self.con.execute('create table linkwords(wordid,linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()


    # Auxilliary function for getting an entry id and adding
    # it if it's not present
    def getentryid(self,table,field,value,createnew=True):
        query = (" select rowid from %s where %s = '%s'" %(table, field, value))
        cur = self.con.execute(query)
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute(
            "insert into %s (%s) values ('%s')" %( table, field, value))
            return cur.lastrowid
        else:
            return res[0]



    # Index an individual page
    def addtoindex(self,url,soup):

        if self.isindexed(url): return
        print ('Indexing %s' % url)


        #getting individual words
        text = self.gettextonly(soup)
        words = self.separatewords(text)


        #get the URL id
        url_id = self.getentryid('urllist', 'url' , url)

        # link each of the words to this url
        for i in range(len(words)):
            word = words[i]
            if (word in ignore_words): continue
            word_id = self.getentryid('wordlist' , 'word', word)
            self.con.execute("insert into wordlocation(urlid,wordid,location) \
            values (%d,%d,%d)" % (url_id, word_id, i))



        # Extract the text from an HTML page (no tags)


    def delete_data(self):
        self.con.execute("delete from wordlist")

    def gettextonly(self, soup):
        v = soup.string
        if v == None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext + '\n'
            return resulttext
        else:
            return v.strip()

    # Separate the words by any non-whitespace character
    def separatewords(self, text):
        splitter = re.compile('\\W+', )
        return [s.lower() for s in splitter.split(text) if s != '']

    # Return true if this url is already indexed
    def isindexed(self, url):
        u = self.con.execute \
            ("select rowid from urllist where url='%s'" % url).fetchone()
        if u != None:
            #Check if it has actually been crawled
            v = self.con.execute(
                'select * from wordlocation where urlid=%d' % u[0]).fetchone()
            if v != None: return True
        return False


    # Add a link between two pages
    def addlinkref(self, urlFrom, urlTo, linkText):
        pass


    # Starting with a list of pages, do a breadth
    # first search to the given depth, indexing pages
    # as we go
    def crawl(self, pages, depth=2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib.request.urlopen(page)
                except:
                    print("Could not open %s", page)
                    continue
                soup = BeautifulSoup(c.read())
                self.addtoindex(page, soup)

                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urljoin(page, link['href'])
                        if (url.find("'") != -1): continue
                        url = url.split("#")[0]
                        if (url[0:4] == 'http' and not self.isindexed(url)):
                            newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)
                self.dbcommit()
                pages = newpages


    '''
    END OF CRAWLER CLASS
    '''


class searcher:
    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def getmatchrows(self, q):
        # Strings to build the query
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        # Split the words by spaces
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            # Get the word ID
            wordrow = self.con.execute(
                "select rowid from wordlist where word='%s'" % word).fetchone()
            if wordrow is not None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist = clauselist +  ' and '
                    clauselist = clauselist  + 'w%d.urlid=w%d.urlid and ' % (tablenumber - 1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1






        # Create the query from the separate parts


        fullquery = 'select %s from %s where %s' % (fieldlist, tablelist, clauselist)
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]

        return rows, wordids


    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0], 0) for row in rows])

        weights = [(1.0, self.word_frequency_score(rows)),
                   (1.5, self.locationscore(rows)),
                   (0.8, self.distance_score(rows))]

        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight*scores[url]

        return totalscores


    def geturlname(self, id):
        return self.con.execute("select url from urllist where rowid=%d" % id).fetchone()[0]

    def query(self, q):
        rows, word_ids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, word_ids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
        for (score, urlid) in rankedscores[0:10]:
            print('%f\t%s' % (score, self.geturlname(urlid)))


    def word_frequency_score(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows:
            counts[row[0]] += 1
        return normalize_scores(counts)

    def locationscore(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]: locations[row[0]] = loc
        return normalize_scores(locations, True)

    def distance_score(self, rows):
        # return 1.0 score for all results if there is only one word in query since there is no distance between words
        if len(rows[0]) <= 2: return dict([(row[0], 1.0) for row in rows])
        # Initialize the dictionary with large values
        mindistance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i] - row[i - 1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]: mindistance[row[0]] = dist
        return normalize_scores(mindistance, True)











