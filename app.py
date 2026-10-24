#!/usr/bin/env python3
"""Illumina Navigator v0.2+0.3: independent sharded FTS and persistent crawl queue."""
import argparse
import concurrent.futures
import hashlib
import ipaddress
import json
import os
import re
import socket
import sqlite3
import threading
import io
import zipfile
from collections import Counter, defaultdict, deque
import time
from difflib import get_close_matches
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse, quote
from urllib.request import Request, urlopen, build_opener, HTTPRedirectHandler
from urllib.robotparser import RobotFileParser

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, 'data')
os.makedirs(DATA, exist_ok=True)
QUEUE_DB = os.path.join(DATA, 'frontier.sqlite3')
SHARDS = 4
AGENT = 'IlluminaNavigatorBot/0.6.1 (+local user-controlled crawler)'
MAX_BYTES = 1_000_000
SETTINGS_FILE = os.path.join(DATA, 'settings.json')
DEFAULT_SETTINGS = {'workers':2,'delay':3,'max_pages':50,'max_hosts':10,'per_host':10,'discover':True,'blocked_hosts':[],'boost_hosts':[]}
settings_lock = threading.RLock()
crawl_lock = threading.Lock()
crawl_state = {'running':False,'last_result':None,'error':None}

def settings_read():
    with settings_lock:
        try:
            with open(SETTINGS_FILE,encoding='utf-8') as f: raw=json.load(f)
            return {**DEFAULT_SETTINGS,**{k:v for k,v in raw.items() if k in DEFAULT_SETTINGS}}
        except (OSError,ValueError): return dict(DEFAULT_SETTINGS)

def settings_save(changes):
    allowed={'workers':(1,10),'delay':(1,60),'max_pages':(1,500),'max_hosts':(1,50),'per_host':(1,100)}
    if not isinstance(changes,dict):raise ValueError('Geçersiz ayarlar')
    with settings_lock:
        current=settings_read()
        for k,v in changes.items():
            if k not in DEFAULT_SETTINGS:raise ValueError('Bilinmeyen ayar: '+k)
            if k in allowed:
                lo,hi=allowed[k]
                if isinstance(v,bool) or not isinstance(v,(int,float)) or not lo<=v<=hi:raise ValueError('Ayar aralık dışında: '+k)
                if k!='delay' and int(v)!=v:raise ValueError('Tam sayı bekleniyor: '+k)
                v=float(v) if k=='delay' else int(v)
            elif k=='discover':
                if not isinstance(v,bool):raise ValueError('Boolean bekleniyor')
            else:
                if not isinstance(v,list) or len(v)>200:raise ValueError('Alan adı listesi çok uzun')
                hosts=[]
                for item in v:
                    if not isinstance(item,str):raise ValueError('Geçersiz alan adı')
                    host=item.strip().lower().rstrip('.')
                    if not re.fullmatch(r'[a-z0-9.-]{1,253}',host) or '..' in host or '.' not in host:raise ValueError('Geçersiz alan adı: '+host)
                    hosts.append(host)
                v=list(dict.fromkeys(hosts))
            current[k]=v
        tmp=SETTINGS_FILE+'.tmp'
        with open(tmp,'w',encoding='utf-8') as f:json.dump(current,f,indent=2,ensure_ascii=False)
        os.replace(tmp,SETTINGS_FILE)
        return current

def host_blocked(host):
    return host in settings_read()['blocked_hosts']

def export_backup():
    buffer=io.BytesIO()
    with zipfile.ZipFile(buffer,'w',zipfile.ZIP_DEFLATED) as z:
        for n in range(SHARDS):
            path=os.path.join(DATA,f'index-{n}.sqlite3')
            with shard_db(n) as conn:
                snapshot=sqlite3.connect(':memory:')
                conn.backup(snapshot)
                sql='\n'.join(snapshot.iterdump())
                z.writestr(f'index-{n}.sql',sql)
                snapshot.close()
        with frontier_db() as conn:
            snapshot=sqlite3.connect(':memory:');conn.backup(snapshot)
            z.writestr('frontier.sql','\n'.join(snapshot.iterdump()))
            snapshot.close()
        z.writestr('settings.json',json.dumps(settings_read(),ensure_ascii=False,indent=2))
        z.writestr('RESTORE.txt','Restore via Python sqlite3.executescript into fresh *.sqlite3 databases while the app is stopped. Review backup contents before restoring.\n')
    return buffer.getvalue()

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None
SAFE_OPENER = build_opener(NoRedirect())
_tls = threading.local()

class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title, self.body, self.links = [], [], []
        self.skip = 0
        self.in_title = False
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in ('script', 'style', 'noscript', 'svg'):
            self.skip += 1
        elif tag == 'title':
            self.in_title = True
        elif tag == 'a' and attrs.get('href'):
            self.links.append(attrs['href'])
    def handle_endtag(self, tag):
        if tag in ('script', 'style', 'noscript', 'svg'):
            self.skip = max(0, self.skip-1)
        elif tag == 'title':
            self.in_title = False
    def handle_data(self, data):
        if not self.skip:
            if self.in_title: self.title.append(data)
            self.body.append(data)

def database(path):
    db = sqlite3.connect(path, timeout=10)
    db.execute('PRAGMA busy_timeout=10000')
    db.execute('PRAGMA journal_mode=WAL')
    return db

def shard(url):
    host = urlparse(url).hostname or ''
    return int.from_bytes(hashlib.sha256(host.encode()).digest()[:4], 'big') % SHARDS

def shard_db(n):
    db = database(os.path.join(DATA, f'index-{n}.sqlite3'))
    db.execute('CREATE TABLE IF NOT EXISTS pages(url TEXT PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL,fetched INTEGER NOT NULL, content_hash TEXT NOT NULL)')
    db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(url UNINDEXED,title,body,tokenize='unicode61')")
    return db

def frontier_db():
    db = database(QUEUE_DB)
    db.execute('CREATE TABLE IF NOT EXISTS jobs(url TEXT PRIMARY KEY, host TEXT NOT NULL, status TEXT NOT NULL DEFAULT \'pending\', attempts INTEGER NOT NULL DEFAULT 0, updated INTEGER NOT NULL DEFAULT 0)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status,updated)')
    return db

def normalize(url):
    p = urlparse(url)
    if p.scheme not in ('https', 'http') or not p.hostname or p.username or p.password:
        return None
    try:
        port = p.port
    except ValueError:
        return None
    if port not in (None, 80, 443):
        return None
    netloc = p.hostname.lower() + ((':' + str(port)) if port else '')
    path = quote(p.path or '/', safe="/:%@!$&'()*+,;=-._~")
    query = quote(p.query, safe="=&%/:?@!$'()*+,;~-._")
    return urlunparse((p.scheme.lower(), netloc, path, '', query, ''))

def public_host(host):
    if not host or host == 'localhost' or host.endswith(('.localhost','.local','.internal')):
        return False
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        return bool(infos) and all(ipaddress.ip_address(x[4][0]).is_global for x in infos)
    except (socket.gaierror, ValueError):
        return False

def add_page(url,title,body):
    url = normalize(url)
    if not url or host_blocked(urlparse(url).hostname): return
    title = title[:250]; body = body[:100000]
    digest = hashlib.sha256((title+'\n'+body).encode()).hexdigest()
    with shard_db(shard(url)) as db:
        old = db.execute('SELECT content_hash FROM pages WHERE url=?',(url,)).fetchone()
        if old and old[0] == digest:
            db.execute('UPDATE pages SET fetched=? WHERE url=?',(int(time.time()),url))
            return
        db.execute('INSERT OR REPLACE INTO pages VALUES(?,?,?,?,?)',(url,title,body,int(time.time()),digest))
        db.execute('DELETE FROM pages_fts WHERE url=?',(url,))
        db.execute('INSERT INTO pages_fts(url,title,body) VALUES(?,?,?)',(url,title,body))

def local_search(query,limit=12):
    words = re.findall(r'[^\W_]+',query,re.UNICODE)[:12]
    if not words: return [],None
    match=' OR '.join('"'+w.replace('"','""')+'"' for w in words)
    all_rows=[]; terms=set()
    for i in range(SHARDS):
        with shard_db(i) as db:
            results=db.execute('''SELECT f.url,f.title,snippet(pages_fts,2,'','',' … ',24),bm25(pages_fts,0,7,1),p.fetched
                FROM pages_fts f JOIN pages p ON f.url=p.url WHERE pages_fts MATCH ? ORDER BY bm25(pages_fts,0,7,1) LIMIT ?''',(match,limit)).fetchall()
            all_rows.extend(results)
            if len(words)<=3:
                for (body,) in db.execute('SELECT title FROM pages LIMIT 150'):
                    terms.update(w.lower() for w in re.findall(r'[^\W_]+',body) if len(w)>2)
    now=time.time(); ranked=[]
    prefs=settings_read()
    for url,title,snippet,bm,fetched in all_rows:
        host=urlparse(url).hostname
        if host in prefs['blocked_hosts']:continue
        # BM25 returns negative relevance; modest freshness boost, not a cross-provider score.
        freshness=max(0,1-(now-fetched)/ (365*86400))
        # Navigational intent: prioritize matching domains and titles; don't show
        # an unrelated encyclopedia page just because it mentions a brand once.
        needle=query.casefold().strip()
        title_hit=needle in title.casefold()
        domain_hit=needle.replace(' ','') in (host or '').casefold()
        url_hit=needle.replace(' ','') in urlparse(url).path.casefold()
        if len(words)==1 and not (title_hit or domain_hit or url_hit):
            # Body-only hits must be substantive, not a passing link/reference.
            with shard_db(shard(url)) as docdb:
                row=docdb.execute('SELECT body FROM pages WHERE url=?',(url,)).fetchone()
            if not row or len(re.findall(r'(?i)(?<!\w)'+re.escape(needle)+r'(?!\w)',row[0]))<3:
                continue
        score=-bm + .025*freshness + (0.75 if host in prefs['boost_hosts'] else 0)
        score+= (8 if domain_hit else 0)+(4 if title_hit else 0)+(1 if url_hit else 0)
        ranked.append((score,{'url':url,'title':title,'description':snippet,'source':'Illumina indeksi'}))
    ranked.sort(key=lambda x:x[0],reverse=True)
    suggestion=None
    if len(words)==1 and words[0].lower() not in terms and terms:
        close=get_close_matches(words[0].lower(),terms,n=1,cutoff=.82)
        if close: suggestion=close[0]
    return [x[1] for x in ranked[:limit]],suggestion

def enqueue(url):
    url=normalize(url)
    if not url or host_blocked(urlparse(url).hostname):return False
    host=urlparse(url).hostname
    with frontier_db() as db:
        result=db.execute("INSERT OR IGNORE INTO jobs(url,host,status,updated) VALUES(?,?,'pending',?)",(url,host,int(time.time())))
    return result.rowcount>0

def crawl_hosts(seeds,workers=2,max_pages=20,delay=2,discover=False,max_hosts=12,recrawl_hours=0,per_host=20):
    """Bounded crawl with opt-in cross-domain discovery and revisits.

    No automatic redirects; every host must have accessible robots.txt.
    """
    seed_urls=[normalize(s) for s in seeds]
    if not seed_urls or any(not u for u in seed_urls):
        raise ValueError('Geçerli HTTP(S) başlangıç URL ver.')
    hosts={}; host_lock=threading.RLock(); last_fetch={}; successful=[0]; reserved=[0]
    host_counts=Counter(); active_jobs=Counter(); per_host=max(1,per_host)
    max_hosts=max(1,min(50,max_hosts)); delay=max(1.0,delay)
    def approve_host(url):
        host=urlparse(url).hostname
        if host_blocked(host):return False
        with host_lock:
            if host in hosts:return hosts[host] is not None
            if len(hosts)>=max_hosts:return False
            # Reserve slot before network activity; failure stays denied this run.
            hosts[host]=None
        if not public_host(host):return False
        p=urlparse(url);rp=RobotFileParser();rp.set_url(f'{p.scheme}://{p.netloc}/robots.txt')
        try:
            # Read with size and timeout cap. A missing/disallowed robots does not grant access.
            with SAFE_OPENER.open(Request(rp.url,headers={'User-Agent':AGENT}),timeout=8) as r:
                if 'text/plain' not in r.headers.get('Content-Type','text/plain'):
                    return False
                content=r.read(256_001)
                if len(content)>256_000:return False
                rp.parse(content.decode('utf-8',errors='replace').splitlines())
        except Exception:return False
        with host_lock:hosts[host]=rp
        return True
    approved_seeds=0
    for u in seed_urls:
        if not approve_host(u):
            print('Başlangıç sitesi atlandı (robots/ağ):',u,flush=True)
            continue
        enqueue(u);approved_seeds+=1
    if approved_seeds==0:
        raise ValueError('Hiçbir başlangıç sitesi robots/ağ kontrolünü geçemedi')
    if recrawl_hours>0:
        older_than=int(time.time()-recrawl_hours*3600)
        with frontier_db() as db:
            db.execute("UPDATE jobs SET status='pending' WHERE status='done' AND updated<?",(older_than,))
    with frontier_db() as db:
        db.execute("UPDATE jobs SET status='pending' WHERE status='running'")
    def worker():
        while True:
            with host_lock:
                # Each reservation consumes one run slot; no parallel worker can overshoot.
                if reserved[0]>=max_pages:return
                active=[h for h,r in hosts.items() if r is not None and host_counts[h]+active_jobs[h]<per_host]
                if not active:return
                with frontier_db() as db:
                    marks=','.join('?'*len(active))
                    candidates=db.execute(f"SELECT url,host FROM jobs WHERE status='pending' AND host IN ({marks}) ORDER BY updated LIMIT 300",active).fetchall()
                    candidates=[j for j in candidates if host_counts[j[1]]+active_jobs[j[1]]<per_host]
                    if not candidates:return
                    # Prefer hosts with fewer pages, avoiding one domain monopolizing the run.
                    url,host=min(candidates,key=lambda j:(host_counts[j[1]]+active_jobs[j[1]],j[0]))
                    db.execute("UPDATE jobs SET status='running',attempts=attempts+1,updated=? WHERE url=?",(int(time.time()),url))
                reserved[0]+=1;active_jobs[host]+=1
                wait=max(0,delay-(time.monotonic()-last_fetch.get(host,0)))
                if wait:time.sleep(wait)
                last_fetch[host]=time.monotonic()
            status='failed'
            try:
                if not hosts[host].can_fetch(AGENT,url):status='blocked'
                elif not public_host(host):status='blocked'
                else:
                    req=Request(url,headers={'User-Agent':AGENT,'Accept':'text/html'})
                    response=None
                    target=url
                    for hop in range(5):
                        try:
                            response=SAFE_OPENER.open(Request(target,headers={'User-Agent':AGENT,'Accept':'text/html'}),timeout=8)
                            break
                        except HTTPError as redir:
                            if redir.code not in (301,302,303,307,308):raise
                            dest=normalize(urljoin(target,redir.headers.get('Location','')))
                            # Never silently cross host/robots boundary.
                            if not dest or urlparse(dest).hostname!=host or not hosts[host].can_fetch(AGENT,dest):raise
                            target=dest
                    if response is None:raise ValueError('Çok fazla yönlendirme')
                    with response as resp:
                        if 'text/html' not in resp.headers.get('Content-Type',''):
                            status='skipped'
                        else:
                            raw=resp.read(MAX_BYTES+1)
                            if len(raw)>MAX_BYTES:status='skipped'
                            else:
                                charset=resp.headers.get_content_charset() or 'utf-8'
                                parser=PageParser();parser.feed(raw.decode(charset,errors='replace'))
                                title=' '.join(' '.join(parser.title).split()) or url
                                body=' '.join(' '.join(parser.body).split())
                                add_page(target,title,body)
                                with host_lock:
                                    successful[0]+=1;host_counts[host]+=1
                                    print(f'[{successful[0]}/{max_pages}] {title[:70]}',flush=True)
                                for href in parser.links[:100]:
                                    link=normalize(urljoin(target,href))
                                    if not link:continue
                                    other=urlparse(link).hostname
                                    if other!=host and not discover:continue
                                    if other!=host and not approve_host(link):continue
                                    enqueue(link)
                                status='done'
            except (HTTPError,URLError,TimeoutError,UnicodeError,ValueError,OSError) as e:
                print('Atlandı:',url,str(e)[:100],flush=True)
            finally:
                with frontier_db() as db:
                    db.execute('UPDATE jobs SET status=?,updated=? WHERE url=?',(status,int(time.time()),url))
                with host_lock:active_jobs[host]-=1
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(10,max(1,workers))) as pool:
        list(pool.map(lambda _:worker(),range(min(10,max(1,workers)))))
    return successful[0]


# Diverse, independently fetched starting points. No external search index/API.
DISCOVERY_SEEDS = [
    'https://www.wikipedia.org/',
    'https://en.wikipedia.org/wiki/Main_Page',
    'https://tr.wikipedia.org/wiki/Anasayfa',
    'https://www.python.org/',
    'https://www.sqlite.org/',
    'https://www.nasa.gov/',
    'https://www.gnu.org/',
    'https://www.w3.org/',
    'https://www.britannica.com/',
    'https://developer.mozilla.org/',
]
AUTO_FILE=os.path.join(DATA,'auto.json')
def auto_enabled():
    try:
        with open(AUTO_FILE,encoding='utf8') as f:return bool(json.load(f).get('enabled',True))
    except (OSError,ValueError,TypeError):return True

def set_auto(value):
    with open(AUTO_FILE+'.tmp','w',encoding='utf8') as f:json.dump({'enabled':bool(value)},f)
    os.replace(AUTO_FILE+'.tmp',AUTO_FILE)

def start_crawl(urls=None, automatic=False):
    if crawl_state['running'] or not crawl_lock.acquire(blocking=False):return False
    cfg=settings_read()
    crawl_state.update(running=True,error=None,last_result=None,automatic=automatic)
    def job():
        try:
            result=crawl_hosts(urls or DISCOVERY_SEEDS,workers=min(2,cfg['workers']),
                    max_pages=min(40,cfg['max_pages']),delay=max(2,cfg['delay']),
                    discover=True,max_hosts=max(12,cfg['max_hosts']),per_host=min(8,cfg['per_host']))
            crawl_state['last_result']=result
        except Exception as exc:crawl_state['error']=str(exc)
        finally:crawl_state['running']=False;crawl_lock.release()
    threading.Thread(target=job,daemon=True).start()
    return True

def background_schedule():
    # Startup refresh only, limited by last run and settings. No permanent daemon.
    stamp=os.path.join(DATA,'last-auto-start.txt')
    try:last=float(open(stamp).read().strip())
    except (OSError,ValueError):last=0
    if auto_enabled() and time.time()-last>24*3600:
        with open(stamp,'w') as f:f.write(str(time.time()))
        start_crawl(automatic=True)

class Handler(BaseHTTPRequestHandler):
    def _reply(self,data,kind='application/json; charset=utf-8',code=200,headers=None):
        self.send_response(code)
        self.send_header('Content-Type',kind)
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Security-Policy',"default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'; base-uri 'none'; object-src 'none'")
        for k,v in (headers or {}).items():self.send_header(k,v)
        self.send_header('Content-Length',str(len(data)))
        self.end_headers();self.wfile.write(data)
    def _json(self,obj,code=200):self._reply(json.dumps(obj,ensure_ascii=False).encode(),code=code)
    def do_GET(self):
        parsed=urlparse(self.path)
        if parsed.path=='/':
            self._reply((PathLikeHTML()).encode(),'text/html; charset=utf-8');return
        if parsed.path=='/favicon.ico':self._reply(b'','image/x-icon');return
        if parsed.path=='/healthz':self._reply(b'OK','text/plain; charset=utf-8');return
        if parsed.path=='/api/settings':self._json(settings_read());return
        if parsed.path=='/api/auto':self._json({'enabled':auto_enabled()});return
        if parsed.path=='/api/crawl-status':self._json(dict(crawl_state));return
        if parsed.path=='/api/backup':
            try:self._reply(export_backup(),'application/zip',headers={'Content-Disposition':'attachment; filename="illumina-navigator-backup.zip"'})
            except Exception as e:self._json({'error':str(e)},500)
            return
        if parsed.path=='/api/search':
            q=parse_qs(parsed.query).get('q',[''])[0].strip()[:200]
            try:
                items,suggestion=local_search(q) if q else ([],None)
                seen=set();results=[]
                for item in items:
                    key=(normalize(item['url']) or item['url']).rstrip('/').lower()
                    if key not in seen:seen.add(key);results.append(item)
                self._json({'results':results,'suggestion':suggestion,'local_count':len(results),'remote_count':0});return
            except Exception as e:self._json({'error':str(e)},500);return
        if parsed.path=='/api/stats':
            counts=[]; domains=set()
            for n in range(SHARDS):
                with shard_db(n) as db:
                    counts.append(db.execute('SELECT COUNT(*) FROM pages').fetchone()[0])
                    domains.update(urlparse(u).hostname for (u,) in db.execute('SELECT url FROM pages'))
            with frontier_db() as db:frontier=dict(db.execute('SELECT status,COUNT(*) FROM jobs GROUP BY status'))
            self._json({'indexed_pages':sum(counts),'indexed_domains':len(domains),'shards':counts,'frontier':frontier});return
        self.send_error(404)
    def do_POST(self):
        # Local management API: browsers from other origins may not mutate settings.
        origin=self.headers.get('Origin','')
        host=self.headers.get('Host','')
        allowed_hosts=('127.0.0.1','localhost','[::1]')
        if host.split(':')[0] not in allowed_hosts or (origin and urlparse(origin).netloc!=host):
            self._json({'error':'Yerel erişim gerekli'},403);return
        try:
            size=int(self.headers.get('Content-Length','0'))
            if size<1 or size>10000:raise ValueError('Geçersiz veri boyutu')
            body=json.loads(self.rfile.read(size))
            if self.path=='/api/auto':
                if not isinstance(body,dict) or not isinstance(body.get('enabled'),bool):raise ValueError('enabled boolean gerekli')
                set_auto(body['enabled'])
                self._json({'enabled':auto_enabled()});return
            if self.path=='/api/refresh':
                if not isinstance(body,dict):raise ValueError('Geçersiz istek')
                if not start_crawl(automatic=False):self._json({'error':'Tarama zaten çalışıyor'},409)
                else:self._json({'started':True},202)
                return
            if self.path=='/api/settings':
                self._json(settings_save(body));return
            if self.path=='/api/crawl':
                if crawl_state['running'] or not crawl_lock.acquire(blocking=False):
                    self._json({'error':'Tarama zaten çalışıyor'},409);return
                try:
                    if not isinstance(body,dict) or not isinstance(body.get('urls'),list) or not 1<=len(body['urls'])<=10:raise ValueError('1-10 başlangıç adresi gerekli')
                    urls=body['urls']
                    if any(not isinstance(x,str) or not normalize(x) for x in urls):raise ValueError('Geçersiz başlangıç adresi')
                    cfg=settings_read()
                    crawl_state.update(running=True,error=None,last_result=None)
                    def job():
                        try:
                            result=crawl_hosts(urls,workers=cfg['workers'],max_pages=cfg['max_pages'],delay=cfg['delay'],discover=cfg['discover'],max_hosts=cfg['max_hosts'],per_host=cfg['per_host'])
                            crawl_state['last_result']=result
                        except Exception as exc:crawl_state['error']=str(exc)
                        finally:crawl_state['running']=False;crawl_lock.release()
                    threading.Thread(target=job,daemon=True).start()
                    self._json({'started':True},202);return
                except Exception:
                    crawl_lock.release();raise
            self.send_error(404)
        except (ValueError,TypeError,json.JSONDecodeError) as e:self._json({'error':str(e)},400)

def PathLikeHTML():
    with open(os.path.join(ROOT,'index.html'),'r',encoding='utf-8') as f:return f.read()

if __name__=='__main__':
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest='command')
    srv=sub.add_parser('serve');srv.add_argument('--port',type=int,default=8765);srv.add_argument('--host',default='127.0.0.1')
    crawl=sub.add_parser('crawl');crawl.add_argument('urls',nargs='+');crawl.add_argument('--max-pages',type=int,default=20);crawl.add_argument('--delay',type=float,default=2);crawl.add_argument('--workers',type=int,default=2);crawl.add_argument('--discover',action='store_true',help='Diğer alan adlarını keşfet');crawl.add_argument('--max-hosts',type=int,default=12);crawl.add_argument('--per-host',type=int,default=20);crawl.add_argument('--recrawl-hours',type=float,default=0);crawl.add_argument('--loop-minutes',type=float,default=0)
    sub.add_parser('stats')
    args=parser.parse_args()
    if args.command=='crawl':
        while True:
            print('Indexed:',crawl_hosts(args.urls,max_pages=max(1,min(500,args.max_pages)),delay=max(1,args.delay),workers=args.workers,discover=args.discover,max_hosts=args.max_hosts,recrawl_hours=max(0,args.recrawl_hours),per_host=max(1,args.per_host)))
            if args.loop_minutes<=0:break
            print('Sonraki döngü bekleniyor (Ctrl+C ile durdur)...',flush=True)
            time.sleep(max(1,args.loop_minutes)*60)
    elif args.command=='stats':
        for n in range(SHARDS):
            with shard_db(n) as db:print(f'Shard {n}:',db.execute('SELECT COUNT(*) FROM pages').fetchone()[0])
        with frontier_db() as db:print('Queue:',dict(db.execute('SELECT status, COUNT(*) FROM jobs GROUP BY status')))
    else:
        if getattr(args,'host','127.0.0.1') not in ('127.0.0.1','localhost'):
            parser.error('v0.5 yalnızca yerel loopback bağlantısını kabul eder')
        background_schedule()
        print('Illumina Navigator: http://'+getattr(args,'host','127.0.0.1')+':'+str(getattr(args,'port',8765)))
        ThreadingHTTPServer((getattr(args,'host','127.0.0.1'),getattr(args,'port',8765)),Handler).serve_forever()
