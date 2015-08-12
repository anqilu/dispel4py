__author__ = 'anqilu'

has_mysqldb = False
try:
    import MySQLdb
    has_mysqldb = True
except ImportError:
    pass

import json
import os

# configuration path for monitoring
home = os.path.expanduser("~") 
CONFIG_PATH = os.path.join(home, "workspace/dispel4py/config.json")
try:
    MONITOR_CONFIGS = json.load(open(CONFIG_PATH))
except StandardError, e:
    print "Cannot locate configuration file for monitor"

    
class Monitor:

    def __init__(self, profiles, args, workflow):
        self.profiles = profiles
        self.cleaned_profiles = {}
        self.pe_process_map = {}
        self.args = args
        self.workflow = workflow

    def get_pe_process_map(self, map, wf_id):
        module_path = self.args.module
        module_name_full = os.path.split(module_path)[-1]
        module_name = os.path.splitext(module_name_full)[0]
        map.setdefault(module_name, [wf_id])

        self.pe_process_map = map

    def get_data_from_profiles(self):
        cleaned_profiles = {}
        process_pe_map = dict_invert(self.pe_process_map)
        # print process_pe_map

        proc_num = len(process_pe_map.keys()) - 1

        for (key, value) in self.profiles.items():
            [category, mark] = key.split("_")

            try:
                if int(mark) in process_pe_map:
                    p_owner = process_pe_map[int(mark)]

            except ValueError:
                if is_hex(mark):
                    # check if it is the workflow exec time attr
                    p_owner = process_pe_map[mark]
                    wf_name = p_owner

            pe_char = cleaned_profiles.setdefault(p_owner, {})
            pei_char = pe_char.setdefault(mark, {})
            pei_char[category] = value

        print cleaned_profiles
        self.cleaned_profiles = cleaned_profiles
        return wf_name, proc_num

    def analyse_and_record(self):
        wf_name, proc_num = self.get_data_from_profiles()

        # store performance data into mysql database if mysqldb imported successfully
        if has_mysqldb:
            db_config = MONITOR_CONFIGS["mysql_db_config"]

            conn = MySQLdb.connect(host=db_config["host"],
                           user=db_config["user"],
                           passwd=db_config["passwd"],
                           db=db_config["db"])

            with conn:
                success = self.record_wf_profile(conn, wf_name, proc_num)
                success = self.record_pe_profile(conn, wf_name) if success else success

            if not success:
                print "Failed to record performance data of current workflow ", wf_name

        else:
            print "Trying to write performance data to file"
            # TODO store workflow performance data to files on disk


    def record_wf_profile(self, conn, wf_name, proc_num):
        wf_id = self.cleaned_profiles[wf_name].keys()[0]
        import binascii
        wf_id_bin = binascii.unhexlify(wf_id)
        assert(is_hex(wf_id))
        wf_mapping = self.args.target
        wf_iter_num = self.args.iter
        graph_parent_path = MONITOR_CONFIGS["graph_file_store"]
        wf_graph = os.path.join(graph_parent_path, wf_id)
        wf_total_time = self.cleaned_profiles[wf_name][wf_id]["exec"]
        wf_sub_time = self.cleaned_profiles[wf_name][wf_id]["submitted"]

        # convert graph to json in node link format
        from dispel4py.workflow_graph import drawDot, draw
        dot_data = draw(self.workflow)
        # store dot file to graph data path
        with open(wf_graph, "w") as outfile:
            outfile.write(dot_data)

        # store png file if store_png set to true
        if MONITOR_CONFIGS.get("store_png", False):
            png_outfile = wf_graph + ".png"
            drawDot(self.workflow, png_outfile)

        cursor = conn.cursor()

        try:
            cursor.execute(
                "INSERT INTO WorkflowProfiles("
                "WF_SubmissionID, WF_Name, WF_Mapping, "
                "WF_ProcessorNum, WF_IterationNum, WF_TotalTime, "
                "WF_Submitted, WF_GraphDescription) " +
                "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
                (wf_id_bin, wf_name, wf_mapping, proc_num, wf_iter_num, wf_total_time, wf_sub_time, wf_graph)
            )
            conn.commit()
            return True

        except StandardError, e:
            print(cursor._last_executed)
            print e
            conn.rollback()
            return False

    def record_pe_profile(self, conn, wf_name):
        wf_id = self.cleaned_profiles[wf_name].keys()[0]
        import binascii
        wf_id_bin = binascii.unhexlify(wf_id)

        cursor = conn.cursor()

        pei_insert_stat = (
            "INSERT INTO PEInstanceProfiles("
            "PEI_Rank, PEI_PEID, PEI_SubmissionID, "
            "PEI_TotalTime, PEI_ReadTime, PEI_WriteTime, "
            "PEI_ProcessTime, PEI_TerminateTime, PEI_InDataSize, "
            "PEI_OutDataSize, PEI_ReadRate, PEI_WriteRate) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
       )

        # print self.cleaned_profiles
        for (pe_id, pe_p) in self.cleaned_profiles.items():
            if pe_id != wf_name:
                pe_total = 0
                pe_read = 0
                pe_write = 0
                pe_process = 0
                pe_terminate = 0
                pe_indatasize = 0
                pe_outdatasize = 0
                pe_indatatype = ""
                pe_outdatatype = ""
                sum_readrate = 0
                sum_writerate = 0

                pei_insert_data_sets = []

                for (pei_rank, pei_p) in pe_p.items():

                    pei_total = pei_p.get("total", 0)
                    pei_read = pei_p.get("read", 0)
                    pei_write = pei_p.get("write", 0)
                    pei_process = pei_p.get("process", 0)
                    pei_terminate = pei_p.get("terminate", 0)
                    pei_indatasize = pei_p.get("indatasize", 0)
                    pei_outdatasize = pei_p.get("outdatasize", 0)
                    pei_readrate = pei_p.get("readrate", 0)
                    pei_writerate = pei_p.get("writerate", 0)

                    pe_total += pei_total
                    pe_read += pei_read
                    pe_write += pei_write
                    pe_process += pei_process
                    pe_terminate += pei_terminate
                    pe_indatasize += pei_indatasize
                    pe_outdatasize += pei_outdatasize
                    sum_readrate += pei_readrate
                    sum_writerate += pei_writerate

                    if pe_indatatype == "":
                        pe_indatatype = pei_p.get("indatatype", "")

                    if pe_outdatatype == "":
                        pe_outdatatype = pei_p.get("outdatatype", "")

                    insert_data = (pei_rank, pe_id, wf_id_bin,
                                   pei_total, pei_read, pei_write, pei_process,
                                   pei_terminate, pei_indatasize, pei_outdatasize,
                                   pei_readrate, pei_writerate)
                    pei_insert_data_sets.append(insert_data)

                try:
                    cursor.execute(
                        "INSERT INTO PEProfiles("
                        "PE_PEID, PE_SubmissionID, PE_TotalTime, "
                        "PE_ReadTime, PE_WriteTime, PE_ProcessTime, "
                        "PE_TerminateTime, PE_InDataSize, PE_OutDataSize, "
                        "PE_InDataType, PE_OutDataType, PE_ReadRate, "
                        "PE_WriteRate) " +
                        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (pe_id, wf_id_bin, pe_total,
                         pe_read, pe_write, pe_process,
                         pe_terminate, pe_indatasize, pe_outdatasize,
                         pe_indatatype, pe_outdatatype, sum_readrate / len(pe_p),
                         sum_writerate / len(pe_p))
                    )

                    for pei_insert_data in pei_insert_data_sets:
                        cursor.execute(
                            pei_insert_stat,
                            pei_insert_data
                        )

                except StandardError, e:
                    print(cursor._last_executed)
                    print e
                    conn.rollback()
                    return False
        return True


def dict_invert(d):
    inv = {}
    for k, vlist in d.iteritems():
        for v in vlist:
            if not inv.has_key(v):
                inv[v] = k
            else:
                import warnings
                warnings.warn("Process %s is allocated to different processing elements : %s, %s." % (v, inv.get(v), k))
                import sys
                sys.exit()
    return inv


def is_hex(s):
    import string
    hex_digits = set(string.hexdigits)
    return all(c in hex_digits for c in s)


def calls_count(f):
    def wrapped(*args, **kwargs):
        # print f.__name__
        if isinstance(args[0], object):
            if hasattr(args[0], "_read_calls") and f.__name__ == "_read":
                args[0]._read_calls += 1
            if hasattr(args[0], "_write_calls") and f.__name__ == "_write":
                args[0]._write_calls += 1
                # print ("Rank [%s] has written %s times." %(args[0].pe.rank, args[0]._write_calls))
        return f(*args, **kwargs)
    return wrapped
