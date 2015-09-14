import commands
import httplib
import json
import sys
import time
import urllib2
#from multiprocessing import Pool

if len(sys.argv) != 7:
    sys.stderr.write('Not enough (or too many) arguments\n')
    sys.exit(1)

hostname = sys.argv[1].partition('.')[0]
environment = sys.argv[2]
application = sys.argv[3]
prefix = '{0}.{1}.{2}'.format(environment, hostname, application)

host = sys.argv[4]
port = sys.argv[5]
omit_jvm_stats = sys.argv[6].lower() == 'true'

timestamp_millis = int(round(time.time() * 1000))
timestamp = timestamp_millis / 1000

def escape_core(str):
    return str.replace('.','_')

def dispatch_value(core_name, field, value, ts, q=None):
    str = '{0}.{1}.{2} {3} {4}'.format(prefix, escape_core(core_name), field, value, ts)
    if q is not None:
        q.append(str)
    else:
        print str

def request_and_response_or_bail(method, url, message):
    try:
        return urllib2.urlopen('http://{0}:{1}{2}'.format(host,port,url)).read()
    except:
        sys.stderr.write('{0}\n'.format(message))
        sys.exit(1)

def get_mbeans(json):
    # This sorts out the annoying { solr-mbeans: [ "THING1", { actualContentOfThing1: {} }, "THING2", { actualContentOfThing2: {} } ] }
    # that Solr does by turning it into { mbeans: { THING1: { actualContentOfThing1: {} }, THING2: { actualContentOfThing2: {} } } }
    unconverted_mbeans = json['solr-mbeans']
    mbeans = {}
    name = None
    for thing in unconverted_mbeans:
        if name is None:
            name = thing
        else:
            mbeans[name] = thing
            name = None
    return mbeans

def system_stats(core_name, omit_jvm_stats):
    system_content = request_and_response_or_bail('GET', '/solr/{0}/admin/system?wt=json&_={1}'.format(core_name, timestamp_millis), 'Error while retrieving system stats')
    system_json = json.loads(system_content)
    if not omit_jvm_stats:
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.uptimeMillis', system_json['jvm']['jmx']['upTimeMS'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.memory.free', system_json['jvm']['memory']['raw']['free'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.memory.max', system_json['jvm']['memory']['raw']['max'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.memory.total', system_json['jvm']['memory']['raw']['total'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.memory.used', system_json['jvm']['memory']['raw']['used'], timestamp)
        print '{0}.{1} {2} {3}'.format(prefix, 'jvm.processors', system_json['jvm']['processors'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.committedVirtualMemorySize', system_json['system']['committedVirtualMemorySize'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.freePhysicalMemorySize', system_json['system']['freePhysicalMemorySize'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.freeSwapSpaceSize', system_json['system']['freeSwapSpaceSize'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.maxFileDescriptorCount', system_json['system']['maxFileDescriptorCount'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.openFileDescriptorCount', system_json['system']['openFileDescriptorCount'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.processCpuTime', system_json['system']['processCpuTime'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.systemLoadAverage', system_json['system']['systemLoadAverage'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.totalPhysicalMemorySize', system_json['system']['totalPhysicalMemorySize'], timestamp)
    print '{0}.{1} {2} {3}'.format(prefix, 'system.totalSwapSpaceSize', system_json['system']['totalSwapSpaceSize'], timestamp)

def query_handler_stats(stats, core_name, name, ts, q=None):
    dispatch_value(core_name, '{0}.5minRateReqsPerSecond'.format(name), stats['5minRateReqsPerSecond'], ts, q)
    dispatch_value(core_name, '{0}.15minRateReqsPerSecond'.format(name), stats['15minRateReqsPerSecond'], ts, q)
    dispatch_value(core_name, '{0}.75thPcRequestTime'.format(name), stats['75thPcRequestTime'], ts, q)
    dispatch_value(core_name, '{0}.95thPcRequestTime'.format(name), stats['95thPcRequestTime'], ts, q)
    dispatch_value(core_name, '{0}.999thPcRequestTime'.format(name), stats['999thPcRequestTime'], ts, q)
    dispatch_value(core_name, '{0}.99thPcRequestTime'.format(name), stats['99thPcRequestTime'], ts, q)
    dispatch_value(core_name, '{0}.avgRequestsPerSecond'.format(name), stats['avgRequestsPerSecond'], ts, q)
    dispatch_value(core_name, '{0}.avgTimePerRequest'.format(name), stats['avgTimePerRequest'], ts, q)
    dispatch_value(core_name, '{0}.errors'.format(name), stats['errors'], ts, q)
    dispatch_value(core_name, '{0}.handlerStart'.format(name), stats['handlerStart'], ts, q)
    dispatch_value(core_name, '{0}.medianRequestTime'.format(name), stats['medianRequestTime'], ts, q)
    dispatch_value(core_name, '{0}.requests'.format(name), stats['requests'], ts, q)
    dispatch_value(core_name, '{0}.timeouts'.format(name), stats['timeouts'], ts, q)
    dispatch_value(core_name, '{0}.totalTime'.format(name), stats['totalTime'], ts, q)

def update_handler_stats(stats, core_name, name, ts, q=None):
    dispatch_value(core_name, '{0}.adds'.format(name), stats['adds'], ts, q)
    dispatch_value(core_name, '{0}.autoCommits'.format(name), stats['autocommits'], ts, q)
    dispatch_value(core_name, '{0}.commits'.format(name), stats['commits'], ts, q)
    dispatch_value(core_name, '{0}.cumulativeAdds'.format(name), stats['cumulative_adds'], ts, q)
    dispatch_value(core_name, '{0}.cumulativeDeletesById'.format(name), stats['cumulative_deletesById'], ts, q)
    dispatch_value(core_name, '{0}.cumulativeDeletesByQuery'.format(name), stats['cumulative_deletesByQuery'], ts, q)
    dispatch_value(core_name, '{0}.cumulativeErrors'.format(name), stats['cumulative_errors'], ts, q)
    dispatch_value(core_name, '{0}.deletesById'.format(name), stats['deletesById'], ts, q)
    dispatch_value(core_name, '{0}.deletesByQuery'.format(name), stats['deletesByQuery'], ts, q)
    dispatch_value(core_name, '{0}.docsPending'.format(name), stats['docsPending'], ts, q)
    dispatch_value(core_name, '{0}.errors'.format(name), stats['errors'], ts, q)
    dispatch_value(core_name, '{0}.expungeDeletes'.format(name), stats['expungeDeletes'], ts, q)
    dispatch_value(core_name, '{0}.optimizes'.format(name), stats['optimizes'], ts, q)
    dispatch_value(core_name, '{0}.rollbacks'.format(name), stats['rollbacks'], ts, q)
    dispatch_value(core_name, '{0}.softAutoCommits'.format(name), stats['soft autocommits'], ts, q)
    dispatch_value(core_name, '{0}.transactionLogsTotalNumber'.format(name), stats['transaction_logs_total_number'], ts, q)
    dispatch_value(core_name, '{0}.transactionLogsTotalSize'.format(name), stats['transaction_logs_total_size'], ts, q)

def cache_stats(stats, core_name, name, ts, q=None):
    dispatch_value(core_name, '{0}.cumulativeEvictions'.format(name), stats['cumulative_evictions'], ts, q)
    dispatch_value(core_name, '{0}.cumulativeHitRatio'.format(name), stats['cumulative_hitratio'], ts, q)
    dispatch_value(core_name, '{0}.cumulativeHits'.format(name), stats['cumulative_hits'], ts, q)
    dispatch_value(core_name, '{0}.cumulativeInserts'.format(name), stats['cumulative_inserts'], ts, q)
    dispatch_value(core_name, '{0}.cumulativeLookups'.format(name), stats['cumulative_lookups'], ts, q)
    dispatch_value(core_name, '{0}.evictions'.format(name), stats['evictions'], ts, q)
    dispatch_value(core_name, '{0}.hitRatio'.format(name), stats['hitratio'], ts, q)
    dispatch_value(core_name, '{0}.hits'.format(name), stats['hits'], ts, q)
    dispatch_value(core_name, '{0}.inserts'.format(name), stats['inserts'], ts, q)
    dispatch_value(core_name, '{0}.lookups'.format(name), stats['lookups'], ts, q)
    dispatch_value(core_name, '{0}.size'.format(name), stats['size'], ts, q)
    dispatch_value(core_name, '{0}.warmupTime'.format(name), stats['warmupTime'], ts, q)

def core_stats(core_name):
    q = list()
    ts_millis = int(round(time.time() * 1000))
    ts = ts_millis / 1000
    mbeans_content = request_and_response_or_bail('GET', '/solr/{0}/admin/mbeans?stats=true&wt=json&_={1}'.format(core_name, ts_millis), 'Error while retrieving core stats')
    mbeans_json = get_mbeans(json.loads(mbeans_content))
    searcher_stats = mbeans_json['CORE']['searcher']['stats']
    dispatch_value(core_name, 'numDocs', searcher_stats['numDocs'], ts, q)
    dispatch_value(core_name, 'maxDoc', searcher_stats['maxDoc'], ts, q)
    if 'numSegments' in searcher_stats:
        dispatch_value(core_name, 'numSegments', searcher_stats['numSegments'], ts, q)
    dispatch_value(core_name, 'deletedDocs', searcher_stats['deletedDocs'], ts, q)
    dispatch_value(core_name, 'indexVersion', searcher_stats['indexVersion'], ts, q)
    dispatch_value(core_name, 'warmupTime', searcher_stats['warmupTime'], ts, q)
    core_stats = mbeans_json['CORE']['core']['stats']
    dispatch_value(core_name, 'refCount', core_stats['refCount'], ts, q)
    query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats'], core_name, 'replication', ts, q)
    dispatch_value(core_name, 'replication.indexVersion', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['indexVersion'], ts, q)
    dispatch_value(core_name, 'replication.generation', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['generation'], ts, q)
    if 'lastCycleBytesDownloaded' in mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']:
        dispatch_value(core_name, 'replication.lastCycleBytesDownloaded', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['lastCycleBytesDownloaded'], ts, q)
    if 'previousCycleTimeInSeconds' in mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']:
        dispatch_value(core_name, 'replication.previousCycleTimeInSeconds', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['previousCycleTimeInSeconds'], ts, q)
    if 'timesFailed' in mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']:
        dispatch_value(core_name, 'replication.timesFailed', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['timesFailed'], ts, q)
    if 'timesIndexReplicated' in mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']:
        dispatch_value(core_name, 'replication.timesIndexReplicated', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['timesIndexReplicated'], ts, q)
    if 'downloadSpeed' in mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']:
        dispatch_value(core_name, 'replication.downloadSpeed', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['downloadSpeed'], ts, q)
    if 'bytesDownloaded' in mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']:
        dispatch_value(core_name, 'replication.bytesDownloaded', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['bytesDownloaded'], ts, q)        
    query_handler_stats(mbeans_json['QUERYHANDLER']['/select']['stats'], core_name, 'select', ts, q)
    query_handler_stats(mbeans_json['QUERYHANDLER']['/update']['stats'], core_name, 'update', ts, q)
    update_handler_stats(mbeans_json['UPDATEHANDLER']['updateHandler']['stats'], core_name, 'updateHandler', ts, q)
    cache_stats(mbeans_json['CACHE']['documentCache']['stats'], core_name, 'documentCache', ts, q)
    cache_stats(mbeans_json['CACHE']['fieldValueCache']['stats'], core_name, 'fieldValueCache', ts, q)
    cache_stats(mbeans_json['CACHE']['filterCache']['stats'], core_name, 'filterCache', ts, q)
    if 'nCache' in mbeans_json['CACHE']:
        cache_stats(mbeans_json['CACHE']['nCache']['stats'], core_name, 'nCache', ts, q)
    cache_stats(mbeans_json['CACHE']['perSegFilter']['stats'], core_name, 'perSegFilter', ts, q)
    cache_stats(mbeans_json['CACHE']['queryResultCache']['stats'], core_name, 'queryResultCache', ts, q)
    dispatch_value(core_name, 'fieldCache.entriesCount', mbeans_json['CACHE']['fieldCache']['stats']['entries_count'], ts, q)
    dispatch_value(core_name, 'fieldCache.insanityCount', mbeans_json['CACHE']['fieldCache']['stats']['insanity_count'], ts, q)
    return q

cores_content = request_and_response_or_bail('GET', '/solr/admin/cores?wt=json&indexInfo=true&_={0}'.format(timestamp_millis), 'Error while retrieving cores.')
cores_json = json.loads(cores_content)

core_names = cores_json['status'].keys()

if len(core_names) == 0:
    sys.stderr.write('No cores\n')
    sys.exit(1)

first_core_name = core_names[0]

system_stats(first_core_name, omit_jvm_stats)

for core_name in core_names:
    dispatch_value(core_name, 'indexHeapUsageBytes', cores_json['status'][core_name]['index']['indexHeapUsageBytes'], timestamp)
    dispatch_value(core_name, 'sizeInBytes', cores_json['status'][core_name]['index']['sizeInBytes'], timestamp)
    dispatch_value(core_name, 'segmentCount', cores_json['status'][core_name]['index']['segmentCount'], timestamp)
    results = core_stats(core_name)
    for q in results:
        print q

#if __name__ == '__main__':
#    p = Pool(len(core_names))
#    results = p.map(core_stats, core_names)
#    for q in results:
#        for i in q:
#            print i
