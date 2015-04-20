import commands
import httplib
import json
import sys
import time

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
connection = httplib.HTTPConnection(host, port, timeout = 5)

def tidy_up():
    connection.close()

def escape_core(str):
    return str.replace('.','_')

def dispatch_value(core_name, field, value):
    print '{0}.{1}.{2} {3} {4}'.format(prefix, escape_core(core_name), field, value, timestamp)

def request_and_response_or_bail(method, url, message):
    try:
        connection.request(method, url)
        return connection.getresponse().read()
    except:
        tidy_up()
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

def query_handler_stats(stats, core_name, name):
    dispatch_value(core_name, '{0}.5minRateReqsPerSecond'.format(name), stats['5minRateReqsPerSecond'])
    dispatch_value(core_name, '{0}.15minRateReqsPerSecond'.format(name), stats['15minRateReqsPerSecond'])
    dispatch_value(core_name, '{0}.75thPcRequestTime'.format(name), stats['75thPcRequestTime'])
    dispatch_value(core_name, '{0}.95thPcRequestTime'.format(name), stats['95thPcRequestTime'])
    dispatch_value(core_name, '{0}.999thPcRequestTime'.format(name), stats['999thPcRequestTime'])
    dispatch_value(core_name, '{0}.99thPcRequestTime'.format(name), stats['99thPcRequestTime'])
    dispatch_value(core_name, '{0}.avgRequestsPerSecond'.format(name), stats['avgRequestsPerSecond'])
    dispatch_value(core_name, '{0}.avgTimePerRequest'.format(name), stats['avgTimePerRequest'])
    dispatch_value(core_name, '{0}.errors'.format(name), stats['errors'])
    dispatch_value(core_name, '{0}.handlerStart'.format(name), stats['handlerStart'])
    dispatch_value(core_name, '{0}.medianRequestTime'.format(name), stats['medianRequestTime'])
    dispatch_value(core_name, '{0}.requests'.format(name), stats['requests'])
    dispatch_value(core_name, '{0}.timeouts'.format(name), stats['timeouts'])
    dispatch_value(core_name, '{0}.totalTime'.format(name), stats['totalTime'])

def update_handler_stats(stats, core_name, name):
    dispatch_value(core_name, '{0}.adds'.format(name), stats['adds'])
    dispatch_value(core_name, '{0}.autoCommits'.format(name), stats['autocommits'])
    dispatch_value(core_name, '{0}.commits'.format(name), stats['commits'])
    dispatch_value(core_name, '{0}.cumulativeAdds'.format(name), stats['cumulative_adds'])
    dispatch_value(core_name, '{0}.cumulativeDeletesById'.format(name), stats['cumulative_deletesById'])
    dispatch_value(core_name, '{0}.cumulativeDeletesByQuery'.format(name), stats['cumulative_deletesByQuery'])
    dispatch_value(core_name, '{0}.cumulativeErrors'.format(name), stats['cumulative_errors'])
    dispatch_value(core_name, '{0}.deletesById'.format(name), stats['deletesById'])
    dispatch_value(core_name, '{0}.deletesByQuery'.format(name), stats['deletesByQuery'])
    dispatch_value(core_name, '{0}.docsPending'.format(name), stats['docsPending'])
    dispatch_value(core_name, '{0}.errors'.format(name), stats['errors'])
    dispatch_value(core_name, '{0}.expungeDeletes'.format(name), stats['expungeDeletes'])
    dispatch_value(core_name, '{0}.optimizes'.format(name), stats['optimizes'])
    dispatch_value(core_name, '{0}.rollbacks'.format(name), stats['rollbacks'])
    dispatch_value(core_name, '{0}.softAutoCommits'.format(name), stats['soft autocommits'])
    dispatch_value(core_name, '{0}.transactionLogsTotalNumber'.format(name), stats['transaction_logs_total_number'])
    dispatch_value(core_name, '{0}.transactionLogsTotalSize'.format(name), stats['transaction_logs_total_size'])

def cache_stats(stats, core_name, name):
    dispatch_value(core_name, '{0}.cumulativeEvictions'.format(name), stats['cumulative_evictions'])
    dispatch_value(core_name, '{0}.cumulativeHitRatio'.format(name), stats['cumulative_hitratio'])
    dispatch_value(core_name, '{0}.cumulativeHits'.format(name), stats['cumulative_hits'])
    dispatch_value(core_name, '{0}.cumulativeInserts'.format(name), stats['cumulative_inserts'])
    dispatch_value(core_name, '{0}.cumulativeLookups'.format(name), stats['cumulative_lookups'])
    dispatch_value(core_name, '{0}.evictions'.format(name), stats['evictions'])
    dispatch_value(core_name, '{0}.hitRatio'.format(name), stats['hitratio'])
    dispatch_value(core_name, '{0}.hits'.format(name), stats['hits'])
    dispatch_value(core_name, '{0}.inserts'.format(name), stats['inserts'])
    dispatch_value(core_name, '{0}.lookups'.format(name), stats['lookups'])
    dispatch_value(core_name, '{0}.size'.format(name), stats['size'])
    dispatch_value(core_name, '{0}.warmupTime'.format(name), stats['warmupTime'])

def core_stats(core_name):
    mbeans_content = request_and_response_or_bail('GET', '/solr/{0}/admin/mbeans?stats=true&wt=json&_={1}'.format(core_name, timestamp_millis), 'Error while retrieving core stats')
    mbeans_json = get_mbeans(json.loads(mbeans_content))
    searcher_stats = mbeans_json['CORE']['searcher']['stats']
    dispatch_value(core_name, 'numDocs', searcher_stats['numDocs'])
    dispatch_value(core_name, 'maxDoc', searcher_stats['maxDoc'])
    #dispatch_value(core_name, 'numSegments', searcher_stats['numSegments'])
    dispatch_value(core_name, 'deletedDocs', searcher_stats['deletedDocs'])
    dispatch_value(core_name, 'indexVersion', searcher_stats['indexVersion'])
    dispatch_value(core_name, 'warmupTime', searcher_stats['warmupTime'])
    core_stats = mbeans_json['CORE']['core']['stats']
    dispatch_value(core_name, 'refCount', core_stats['refCount'])
    #query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.component.SearchHandler']['stats'], 'search', core_name)
    #query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.UpdateRequestHandler']['stats'], 'updateRequest, core_name)
    query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats'], core_name, 'replication')
    dispatch_value(core_name, 'replication.indexVersion', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['indexVersion'])
    dispatch_value(core_name, 'replication.generation', mbeans_json['QUERYHANDLER']['org.apache.solr.handler.ReplicationHandler']['stats']['generation'])
    query_handler_stats(mbeans_json['QUERYHANDLER']['/select']['stats'], core_name, 'select')
    query_handler_stats(mbeans_json['QUERYHANDLER']['/update']['stats'], core_name, 'update')
    #query_handler_stats(mbeans_json['QUERYHANDLER']['dismax']['stats'], core_name, 'dismax')
    #query_handler_stats(mbeans_json['QUERYHANDLER']['standard']['stats'], core_name, 'standard')
    #query_handler_stats(mbeans_json['QUERYHANDLER']['warming']['stats'], core_name, 'warming')
    query_handler_stats(mbeans_json['QUERYHANDLER']['org.apache.solr.handler.UpdateRequestHandler']['stats'], core_name, 'updateRequest')
    update_handler_stats(mbeans_json['UPDATEHANDLER']['updateHandler']['stats'], core_name, 'updateHandler')
    cache_stats(mbeans_json['CACHE']['documentCache']['stats'], core_name, 'documentCache')
    cache_stats(mbeans_json['CACHE']['fieldValueCache']['stats'], core_name, 'fieldValueCache')
    cache_stats(mbeans_json['CACHE']['filterCache']['stats'], core_name, 'filterCache')
    if mbeans_json['CACHE']['nCache'] is not None:
        cache_stats(mbeans_json['CACHE']['nCache']['stats'], core_name, 'nCache')
    cache_stats(mbeans_json['CACHE']['queryResultCache']['stats'], core_name, 'queryResultCache')
    dispatch_value(core_name, 'fieldCache.entriesCount', mbeans_json['CACHE']['fieldCache']['stats']['entries_count'])
    dispatch_value(core_name, 'fieldCache.insanityCount', mbeans_json['CACHE']['fieldCache']['stats']['insanity_count'])

cores_content = request_and_response_or_bail('GET', '/solr/admin/cores?wt=json&indexInfo=true&_={0}'.format(timestamp_millis), 'Error while retrieving cores.')
cores_json = json.loads(cores_content)

core_names = cores_json['status'].keys()

if len(core_names) == 0:
    tidy_up()
    sys.stderr.write('No cores\n')
    sys.exit(1)

first_core_name = core_names[0]

system_stats(first_core_name, omit_jvm_stats)

for core_name in core_names:
    dispatch_value(core_name, 'indexHeapUsageBytes', cores_json['status'][core_name]['index']['indexHeapUsageBytes'])
    dispatch_value(core_name, 'sizeInBytes', cores_json['status'][core_name]['index']['sizeInBytes'])
    dispatch_value(core_name, 'segmentCount', cores_json['status'][core_name]['index']['segmentCount'])
    core_stats(core_name)

tidy_up()
