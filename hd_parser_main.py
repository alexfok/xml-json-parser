
import os
import sys
import argparse
import json
import datetime
import db_connection
import xml.etree.ElementTree as ET
import shutil
import logging
import logging.handlers
import pprint
from os.path import basename

#from cpdiag_stat import CPDiagStat
#import cpdiag_general_parser

"""
    CPDiag Parser
    Main features:
	    Read from Oracle and save xml files
	    Parse XML files to aggregated JSON file
	        JSON file can be overwritten or appended
	    Discover xml schema and manages JSON schema file
	    Renames column names based on JSON replace_column file
	    Produces detailed parsing report
"""

# Global Configuration Parameters
LOGGER = None
DEFAULT_DATA_DIR = "data"
DEFAULT_OUT_DATA_DIR = "data"
REPORT_SCHEMA_FILE="report_schema.json"
RENAME_MAP_CONFIG_FILE="rename_map.json"
LOG_FILENAME = 'cpdiag_parser_log.log'
DEFAULT_MODE="all"
DEBUG=False
DEF_ORACLE_ROWS_NUMB='10'
REPORT_SCHEMA=[]
DEF_RENAME_MAP = {"read I/Os": "read_IOs", "write I/Os": "write_IOs", "read merges": "read_merges",
                  "write merges": "write_merges", "read sectors": "read_sectors", "write sectors": "write_sectors",
                  "read ticks": "read_ticks", "write ticks": "write_ticks"}

def write_log(s, loglevel=logging.INFO):
    if loglevel==logging.DEBUG:
        logging.getLogger(__name__).debug(s)
    elif loglevel==logging.INFO:
        logging.getLogger(__name__).info(s)
    elif loglevel == logging.WARNING:
        logging.getLogger(__name__).warning(s)
    elif loglevel == logging.ERROR:
        logging.getLogger(__name__).error(s)
    elif loglevel == logging.CRITICAL:
        logging.getLogger(__name__).critical(s)

#    sys.stderr.write('%s\n' % s)
def init_logger(loglevel=logging.INFO):
    global LOGGER
#    logging.basicConfig(filename='cpdiag_parser.log', level=loglevel)
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)
    # Add the log message handler to the logger
    # Console logger
    logger.addHandler(logging.StreamHandler())
    # Rotating file logger
    handler = logging.handlers.RotatingFileHandler(
        LOG_FILENAME, maxBytes=2000000000, backupCount=5)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info('Logger Initiated')
    write_log('Process Starting', logging.INFO)
    LOGGER = logger

def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z

###########################################
# Utilities
# This method currently is not used
# dic1 = {
#    "a": 1,
#    "b": 2,
#    "c": [2, 3, 4],
#    "d": {"d1":"dv1", "d2":"dv2"}
# }
# {'b': 2, 'd_d1': 'dv1', 'c_1': 3, 'a': 1, 'c_0': 2, 'd_d2': 'dv2', 'c_2': 4}
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

# Load JSON files
def load_data_json_files():
    data_json_files_dict = {}
    dir_path = DEFAULT_DATA_DIR
    for file_name in os.listdir(dir_path):
        if file_name.endswith(".json"):
            file_path = os.path.join(dir_path, file_name)
            write_log ("Loading JSON file " + file_path)
            data_json_file = open(file_path , "r")
            data_json = json.loads(data_json_file)
            data_json_files_dict[file_path] = data_json
#            test_json_file.read(report_json)
            data_json_file.close()
            continue
        else:
            continue
    return data_json_files_dict

def parse_args():
    """Parse the command line for options."""
    global DEBUG
    parser = argparse.ArgumentParser(description='This is HD Parser program')

    parser.add_argument(
        '--data_dir', dest='data_dir', action='store', default=DEFAULT_DATA_DIR,
        help='Provide data dir path (./data)'
    )
    parser.add_argument(
        '--out_data_dir', dest='out_data_dir', action='store', default=DEFAULT_OUT_DATA_DIR,
        help='Provide data dir path (./data)'
    )
    parser.add_argument('--mode', '-m', choices=('read-oracle', 'parse-json', 'query-hadoop', 'all'), default=DEFAULT_MODE, help="Enter the required mode - read reports from Oracle, parse json file and store in Hadoop, make Hadoop query, perform ALL operations (all)")
    parser.add_argument(
        '--debug', '-d', dest='debug', action='store_true', default=False,
        help='Debug info will be printed (False)'
    )
#    '--oracle_rows_numb', dest = 'oracle_rows_numb', action = 'store', type = int, default = DEF_ORACLE_ROWS_NUMB,
    parser.add_argument(
        '--oracle_rows_numb', dest='oracle_rows_numb', action='store', default=DEF_ORACLE_ROWS_NUMB,
        help='oracle_rows_numb (10)'
    )
    parser.add_argument(
        '--overwrite_json', dest='overwrite_json_file', action='store_true', default=False,
        help='Overwrite or append report JSON file (False)'
    )

    parser.add_argument(
        '--loglevel', dest='loglevel', action='store', default=logging.WARNING,
        help='Log level (WARNING)'
    )

    try:
        options = parser.parse_args()
        write_log(options, logging.INFO)
        DEBUG = options.debug
        # positional arguments are ignored
        return options
    except msg:
        parser.error(str(msg))


def parse_cpdiag_event(elem, schema_disc):
    """
    turns an elementtree-compatible object into a pesterfish dictionary
    (not json).

    """
#    d=dict(tag=elem.tag)
    write_log("parse_cpdiag_event start parsing CPDiagEvent elem.elemattrib[keyname]: " + elem.attrib["keyname"])
    d = {}
    d['tag']=elem.tag
    if elem.text:
        d['text']=elem.text
    for key, value in elem.attrib.iteritems():
        d[key] = value
        schema_disc[key] = schema_disc.get(key,0) + 1
    for cpdiag_event_stat in elem.findall("CPDiagEventStat"):
        write_log("parse_cpdiag_event parsing CPDiagEventStat cpdiag_event_stat.attrib[keyname]: " + cpdiag_event_stat.attrib["keyname"])
        d[cpdiag_event_stat.attrib["keyname"]] = cpdiag_event_stat.text
        schema_disc[cpdiag_event_stat.attrib["keyname"]] = schema_disc.get(cpdiag_event_stat.attrib["keyname"], 0) + 1
    write_log("parse_cpdiag_event finished parsing CPDiagEvent elem.elemattrib[keyname]: " + elem.attrib["keyname"] + "\nd: " + str(d))
    return d, schema_disc

def parse_xml_file2(file_path):
    write_log("parse_xml_file2 started")
    report_json = {}
    schema_disc = {}

    report_json['meta_data'] = {}
    report_json['cpdiag_events'] = {}

    write_log("Loading xml file " + file_path + " time: " + str(datetime.datetime.now()))
    tree = ET.parse(file_path)
#    if hasattr(tree, 'getroot'):
    root = tree.getroot()
    if root.tag != "modules":
        raise ValueError("Bad XML root element - root.tag: ", root.tag)

    write_log("parse_xml_file2 parsed xml")

    for module_node in root.findall("module"):
        # module node attributes:
        # CK="A20AD886C553" CPDiagBuildNumber="991100015" GatewayType="host" IP="10.5.53.233" MAC="00:15:5D:CA:2D:01" TimezoneOnMachine="+1030"
#        report_json['attributes'] = module_node.attrib
        for key, value in module_node.attrib.iteritems():
            report_json['meta_data'][key] = value
            schema_disc[key] = schema_disc.get(key, 0) + 1
        for cpdiag_node in module_node.findall("cpdiag"):
            count=0
            for cpdiag_event in cpdiag_node.findall("CPDiagEvent"):
                report_json['cpdiag_events']['cpdiag_event' + '_' + str(count)], schema_disc = parse_cpdiag_event(cpdiag_event, schema_disc)
                count+=1
    write_log("parse_xml_file2 finished - Parsed " + file_path + " time: " + str(datetime.datetime.now()))
    write_log("parse_xml_file2 finished schema str: " + str(schema_disc))
    write_log("parse_xml_file2 finished schema dumps: " + json.dumps(schema_disc))
    if report_json == {}:
        write_log("parse_xmls_to_json finished - empty report_json", logging.WARNING)
    else:
        write_log("parse_xmls_to_json finished report_json: " + str(report_json), logging.DEBUG)
    return report_json, schema_disc

def rename_json_fields(report_json, rep_schema_disc):
    #######################
#    DEF_RENAME_MAP = {"read I/Os": "read_IOs", "write I/Os": "write_IOs", "read merges": "read_merges", "write merges": "write_merges", "read sectors" : "read_sectors" , "write sectors" : "write_sectors" , "read ticks": "read_ticks", "write ticks": "write_ticks"}
    rename_map = DEF_RENAME_MAP
    try:
        with open(RENAME_MAP_CONFIG_FILE) as data_file:
            rename_map = json.load(data_file)
    except:
        write_log("\nUse default rename map - file doesn't exist: " +  RENAME_MAP_CONFIG_FILE, logging.WARNING)
    # Rename columns
    for key_to_replace in rename_map:
        write_log('replacing key: key_to_replace ' + key_to_replace + ' with key: ' + rename_map[key_to_replace], logging.DEBUG)
        if key_to_replace in rep_schema_disc:
            rep_schema_disc[rename_map[key_to_replace]] = rep_schema_disc[key_to_replace]
            del rep_schema_disc[key_to_replace]
            cpdiag_events = report_json['cpdiag_events']
            for key, cpdiag_event in cpdiag_events.iteritems():
                if key_to_replace in cpdiag_event:
                    cpdiag_event[rename_map[key_to_replace]] = cpdiag_event[key_to_replace]
                    del cpdiag_event[key_to_replace]
    write_log("schema_disc after keys replacement: " + str(rep_schema_disc))
    return report_json, rep_schema_disc
    #######################

def init_rep_schema():
    report_schema = {'keys': []}
    try:
        with open(REPORT_SCHEMA_FILE, 'a+') as data_file:
            report_schema = json.load(data_file)
    except:
        write_log("\nNo report schema file, create one: " + REPORT_SCHEMA_FILE, logging.WARNING)
    return report_schema

def merge_rep_schema(rep_schema_disc):
    #######################
    #  Merge discovered report schema
    report_schema = rep_schema_disc
    write_log("\nmerge_rep_schema rep_schema_disc: " + str(rep_schema_disc), logging.DEBUG)
    try:
        report_schema = init_rep_schema()
        # overwrite REPORT_SCHEMA_FILE
        with open(REPORT_SCHEMA_FILE, "w") as data_file:
            # Merge discovered report schema
            for key_to_add in rep_schema_disc:
                if key_to_add not in report_schema['keys']:
                    report_schema['keys'].append(key_to_add)
            # Save the report_schema to the file
            data_file.write(json.dumps(report_schema) + "\n")
            write_log("report_schema: " + str(report_schema), logging.DEBUG)
    except:
        write_log("\nError in report schema file merge: " + REPORT_SCHEMA_FILE, logging.ERROR)
    return report_schema


def parse_xmls_to_json(options):
    """
    Spark
    For all JSON files in data dir
    append to daily JSON file
    copy daily JSON file to HDFS or NFS?
    append daily JSON file to HDFS or NFS?
    discovery schema - file management
    fields rename map file
    """
    msg="Unknown Exception"
    parsed_files_count=0
    total_files_count=0
    xml_files_count=0
    write_log("parse_xmls_to_json started", logging.DEBUG)
    try:
        # Generate JSON report file name
        d = datetime.datetime.utcnow()
#        print(datetime.date.strftime(d, "%m%d%y"))
        dd = datetime.date.strftime(d, "%m%d%y")
        out_file_name = options.out_data_dir + "/" + dd + ".json"
        dir_path = options.data_dir
        # Open\Create JSON report file name
        write_log("JSON Report file name: " + out_file_name)
        if options.overwrite_json_file:
            rep_json_file = open(out_file_name, "w")
        else:
            rep_json_file = open(out_file_name, "a+")
        # Read report schema from file
        rep_schema_disc = init_rep_schema()

        # Loop over all XML files in data directory
        for file_name in os.listdir(dir_path):
            try:
                total_files_count += 1
                if file_name.endswith(".xml"):
                    xml_files_count += 1
                    # Verify XML file name convention:
                    # rep_id_rep_ck_rep_ip_rep_date.xml
                    # "3127643_EE4E1D3DC51D_81.15.241.69_01-09-17.xml"
                    file_path = os.path.join(dir_path, file_name)
                    write_log("XML file_name to parse: "+ file_name)
                    words = file_name.split('_')
                    if len(words) != 4:
                        raise ValueError("Invalid XML file name: ", file_name)
                    # Parse XML - returns report metadata + dictionary of JSON lines for every cpdiagevent
                    report_json, rep_schema_disc = parse_xml_file2(file_path)
                    if report_json:
                        #######################
                        #  Rename JSON fields according to the map
                        report_json, rep_schema_disc = rename_json_fields(report_json, rep_schema_disc)
                        #######################
                        #  Merge discovered report schema
                        rep_schema_disc = merge_rep_schema(rep_schema_disc)
                        # compose report line for every cpdiagevent
                        # json {meta_data, cpdiag_events}
                        #  Merge discovered report schema
                        #  Create meta_data report field
                        # TODO: use meta data fields from JSON file when switching from Oracle to report files
                        meta_data = report_json['meta_data']
                        meta_data['rep_id'] = words[0]
                        meta_data['rep_ck'] = words[1]
                        meta_data['rep_ip'] = words[2]
                        meta_data['rep_date'] = (words[3].split('.'))[0]
                        write_log("File name metadata - rep_id: " + meta_data['rep_id'] + " rep_ck: " + meta_data['rep_ck'] + " rep_ip: " + meta_data['rep_ip'] + " rep_date: " + meta_data['rep_date'], logging.DEBUG)
                        #  Merge discovered report schema
                        cpdiag_events = report_json['cpdiag_events']
                        for key, cpdiag_event in cpdiag_events.iteritems():
                            report_line = merge_two_dicts(meta_data, cpdiag_event)
                            rep_json_file.write(json.dumps(report_line) + "\n")
                        parsed_files_count += 1
                        shutil.copy(file_path, file_path + ".back" + str(parsed_files_count))
            except ValueError as msg:
                write_log("Error Parsing XML File file_name: " + file_name + 'msg' + str(msg), logging.ERROR)
        # Close JSON report file name
        rep_json_file.close()
        write_log("parse_xmls_to_json finished schema_disc: " + str(rep_schema_disc))
# Write XML to file
#            d = datetime.datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S')
#            print(datetime.date.strftime(d, "%m-%d-%y"))
#            dd = datetime.date.strftime(d, "%m-%d-%y")
#            out_file_name="data/" + str(row[0]) + "_" + str(row[3]) + "_" + str(row[4]) + "_" + dd + ".xml"
#            print(xmlstr2json(xml_str))
    except ValueError as msg:
        write_log("Error Reading XML Files msg: " + str(msg), logging.CRITICAL)
    write_log("parse_xmls_to_json finished - total_files_count: " + str(total_files_count) + " xml_files_count: " + str(xml_files_count) + " parsed_files_count: " + str(parsed_files_count))

def read_oracle(options):
    """
    Copy from CPDiagParser
    Read data directory with XML files
    Convert every XML to JSON
    Add meta data to JSON
        1 whole XML to JSON
        2 info part only
        3 save parsed JSON files to out directory
    print parsed lines report
    """
    if (DEBUG): print("read-oracle started, options.oracle_rows_numb: " + options.oracle_rows_numb)
#    cpdiag_parser_dict = load_parsers()
    try:
        command = 'select ID,TIMESTAMP,DATA,CK,IP from UCDEF.LICENSE_INFORMATION where sysdate-1 < TIMESTAMP AND ROWNUM <=' + options.oracle_rows_numb + ' order by TIMESTAMP'
        oracle_conn = db_connection.OracleConnection()
        for row in oracle_conn.run_query(command):
#            cpdiag_stat = CPDiagStat()
#            cpdiag_stat.user = "user"
#           cpdiag_stat.password = "password"
#            cpdiag_stat.id = row[0]
#            cpdiag_stat.timestamp = row[1]
            xml_str = str(row[2])
#            cpdiag_stat.report_size = len(xml_str) / 1024 / 1024
#            cpdiag_stat.ck = row[3]
#            cpdiag_stat.ip = str(row[4])
#            cpdiag_stat.cpdiag_parser_dict = cpdiag_parser_dict
            #    			print "Read xml from Oracle: " + xml_str
            # Write XML to file
            d = datetime.datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S')
            print(datetime.date.strftime(d, "%m-%d-%y"))
            dd = datetime.date.strftime(d, "%m-%d-%y")
            out_file_name = options.data_dir + "/" + str(row[0]) + "_" + str(row[3]) + "_" + str(row[4]) + "_" + dd + ".xml"
            text_file = open(out_file_name, "w")
            text_file.write(xml_str)
            text_file.close()
            print("Read xml from Oracle: " + str(row[0]))
#            report_json = pparse_xml(cpdiag_stat, xml_str)
#            print(xmlstr2json(xml_str))
        oracle_conn.close()

    except (db_connection.ConnectionError):
        print("Unable to connect to Oracle DB")
    write_log("read-oracle finished", logging.INFO)

def query_hadoop(options):
    """
        Spark
        Read JSON from Hadoop to DF
        Make SQL Queries
    """
    if (DEBUG): print("query-hadoop started")
    if (DEBUG): print("query-hadoop finished")

def all(options):
    read_oracle(options)
    parse_xmls_to_json(options)
    query_hadoop(options)
    print ("all\n")

# map the inputs to the function blocks
modes = {'read-oracle' : read_oracle,
           'parse-json' : parse_xmls_to_json,
           'query-hadoop' : query_hadoop,
           'all' : all
}


def main():
    """hd_parser main"""
    try:
        # create logger
        loglevel = logging.INFO
        options = parse_args()
        # TODO: set loglevel from command line argument
#        numeric_level = getattr(options, options.loglevel, logging.ERROR)
#        print('1 getattr(logging, loglevel.upper()): ' + getattr(logging, loglevel))
#        if isinstance(options.loglevel, int):
#            print('2')
#            loglevel = options.loglevel
#        print('3')
        #################
        # Init logger
        init_logger(loglevel)


        DEBUG = options.debug
        modes[options.mode](options)
#        return parser_main_loop(options)
    except:
        print ("hd_parser_main_loop exception")

if __name__ == "__main__":
    sys.exit(main())
