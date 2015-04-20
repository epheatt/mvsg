import commands
import httplib
import json
import sys
import time
import urllib2
from multiprocessing import Pool

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

def dispatch_value(core_name, field, value, ts):
    print '{0}.{1}.{2} {3} {4}'.format(prefix, escape_core(core_name), field, value, ts)

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

def query_handler_stats(stats, core_name, name, ts):
    dispatch_value(core_name, '{0}.5minRateReqsPerSecond'.format(name), stats['5minRateReqsPerSecond'], ts)
    dispatch_value(core_name, '{0}.15minRateReqsPerSecond'.format(name), stats['15minRateReqsPerSecond'], ts)
    dispatch_value(core_name, '{0}.75thPcRequestTime'.format(name), stats['75thPcRequestTime'], ts)
    dispatch_value(core_name, '{0}.95thPcRequestTime'.format(name), stats['95thPcRequestTime'], ts)
    dispatch_value(core_name, '{0}.999thPcRequestTime'.format(name), stats['999thPcRequestTime'], ts)
    dispatch_value(core_name, '{0}.99thPcRequestTime'.format(name), stats['99thPcRequestTime'], ts)
    dispatch_value(core_name, '{0}.avgRequestsPerSecond'.format(name), stats['avgRequestsPerSecond'], ts)
    dispatch_value(core_name, '{0}.avgTimePerRequest'.format(name), stats['avgTimePerRequest'], ts)
    dispatch_value(core_name, '{0}.errors'.format(name), stats['errors'], ts)
    dispatch_value(core_name, '{0}.handlerStart'.format(name), stats['handlerStart'], ts)
    dispatch_value(core_name, '{0}.medianRequestTime'.format(name), stats['medianRequestTime'], ts)
    dispatch_value(core_name, '{0}.requests'.format(name), stats['requests'], ts)
    dispatch_value(core_name, '{0}.timeouts'.format(name), stats['timeouts'], ts)
    dispatch_value(core_name, '{0}.totalTime'.format(name), stats['totalTime'], ts)

def update_handler_stats(stats, core_name, name, ts):
    dispatch_value(core_name, '{0}.adds'.format(name), stats['adds'], ts)
    dispatch_value(core_name, '{0}.autoCommits'.format(name), stats['autocommits'], ts)
    dispatch_value(core_name, '{0}.commits'.format(name), stats['commits'], ts)
    dispatch_value(core_name, '{0}.cumulativeAdds'.format(name), stats['cumulative_adds'], ts)
    dispatch_value(core_name, '{0}.cumulativeDeletesById'.format(name), stats['cumulative_deletesById'], ts)
    dispatch_value(core_name, '{0}.cumulativeDeletesByQuery'.format(name), stats['cumulative_deletesByQuery'], ts)
    dispatch_value(core_name, '{0}.cumulativeErrors'.format(name), stats['cumulative_errors'], ts)
    dispatch_value(core_name, '{0}.deletesById'.format(name), stats['deletesById'], ts)
    dispatch_value(core_name, '{0}.deletesByQuery'.format(name), stats['deletesByQuery'], ts)
    dispatch_value(core_name, '{0}.docsPending'.format(name), stats['docsPending'], ts)
    dispatch_value(core_name, '{0}.errors'.format(name), stats['errors'], ts)
    dispatch_value(core_name, '{0}.expungeDeletes'.format(name), stats['expungeDeletes'], ts)
    dispatch_value(core_name, '{0}.optimizes'.format(name), stats['optimizes'], ts)
    dispatch_value(core_name, '{0}.rollbacks'.format(name), stats['rollbacks'], ts)
    dispatch_value(core_name, '{0}.softAutoCommits'.format(name), stats['soft autocommits'], ts)
    dispatch_value(core_name, '{0}.transactionLogsTotalNumber'.format(name), stats['transaction_logs_total_number'], ts)
    dispatch_value(core_name, '{0}.transactionLogsTotalSize'.format(name), stats['transaction_logs_total_size'], ts)

def cache_stats(stats, core_name, name, ts):
    dispatch_value(core_name, '{0}.cumulativeEvictions'.format(name), stats['cumulative_evictions'], ts)
    dispatch_value(core_name, '{0}.cumulativeHitRatio'.format(name), stats['cumulative_hitratio'], ts)
    dispatch_value(core_name, '{0}.cumulativeHits'.format(name), stats['cumulative_hits'], ts)
    dispatch_value(core_name, '{0}.cumulativeInserts'.format(name), stats['cumulative_inserts'], ts)
    dispatch_value(core_name, '{0}.cumulativeLookups'.format(name), stats['cumulative_lookups'], ts)
    dispatch_value(core_name, '{0}.evictions'.format(name), stats['evictions'], ts)
    dispatch_value(core_name, '{0}.hitRatio'.format(name), stats['hitratio'], ts)
    dispatch_value(core_name, '{0}.hits'.format(name), stats['hits'], ts)
    dispatch_value(core_name, '{0}.inserts'.format(name), stats['inserts'], ts)
    dispatch_value(core_name, '{0}.lookups'.format(name), stats['lookups'], ts)
    dispatch_value(core_name, '{0}.size'.format(name), stats['size'], ts)
    dispatch_value(core_name, '{0}.warmupTime'.format(name), stats['warmupTime'], ts)

def core_stats(core_name):
    ts_millis = int(round(time.time() * 1000))
    ts = ts_millis / 1000
    mbeans_content = request_and_response_or_bail('GET', '/solr/{0}/admin/mbeans?stats=true&wt=json&_={1}'.format(core_name, ts_millis), 'Error while retrieving core stats')
    mbeans_json = get_mbeans(json.loads(mbeans_content))
    searcher_stats = mbeans_json['CORE']['searcher']['stats']
    dispatch_value(core_name, 'numDocs', searcher_stats['numDocs'], ts)
    dispatch_value(core_name, 'maxDoc', searcher_stats['maxDoc'], ts)
    if 'numSegments' in searcher_stats:
        dispatch_value(core_name, 'numSegments', searcher_stats['numSegments'], ts)
    dispatch_value(core_name, 'deletedDocs', searcher_stats['deletedDocs'], ts)
    dispatch_value(core_name, 'indexVersion', searcher_stats['indexVersion'], ts)
    dispatch_value(core_name, 'warmupTime', searcher_stats['warmupTime'], ts)
    core_stats = mbeans_json['CORE']['core']['stats']
    dispatch_value(core_name, 'refCount', core_stats['refCount'], ts)
    #query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.component.SearchHandler']['stats'], core_name, 'search', ts)
    #query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.UpdateRequestHandler']['stats'], core_name, 'updateRequest, ts)
    query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats'], core_name, 'replication', ts)
    dispatch_value(core_name, 'replication.indexVersion', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['indexVersion'], ts)
    dispatch_value(core_name, 'replication.generation', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['generation'], ts)
    query_handler_stats(mbeans_json['QUERYHANDLER']['/select']['stats'], core_name, 'select', ts)
    query_handler_stats(mbeans_json['QUERYHANDLER']['/update']['stats'], core_name, 'update', ts)
    #query_handler_stats(mbeans_json['QUERYHANDLER']['dismax']['stats'], core_name, 'dismax', ts)
    #query_handler_stats(mbeans_json['QUERYHANDLER']['standard']['stats'], core_name, 'standard', ts)
    #query_handler_stats(mbeans_json['QUERYHANDLER']['warming']['stats'], core_name, 'warming', ts)
    query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.UpdateRequestHandler']['stats'], core_name, 'updateRequest', ts)
    update_handler_stats(mbeans_json['UPDATEHANDLER']['updateHandler']['stats'], core_name, 'updateHandler', ts)
    cache_stats(mbeans_json['CACHE']['documentCache']['stats'], core_name, 'documentCache', ts)
    cache_stats(mbeans_json['CACHE']['fieldValueCache']['stats'], core_name, 'fieldValueCache', ts)
    cache_stats(mbeans_json['CACHE']['filterCache']['stats'], core_name, 'filterCache', ts)
    if 'nCache' in mbeans_json['CACHE']:
        cache_stats(mbeans_json['CACHE']['nCache']['stats'], core_name, 'nCache', ts)
    cache_stats(mbeans_json['CACHE']['queryResultCache']['stats'], core_name, 'queryResultCache', ts)
    dispatch_value(core_name, 'fieldCache.entriesCount', mbeans_json['CACHE']['fieldCache']['stats']['entries_count'], ts)
    dispatch_value(core_name, 'fieldCache.insanityCount', mbeans_json['CACHE']['fieldCache']['stats']['insanity_count'], ts)

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

if __name__ == '__main__':
    p = Pool(len(core_names))
    p.map(core_stats, core_names)
