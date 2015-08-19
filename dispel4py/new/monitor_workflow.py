__author__ = 'anqilu'

import json
import os
import time
import warnings
import subprocess
import binascii

from multiprocessing import Process, Pipe

has_mysqldb = False
has_csv = False
try:
    import MySQLdb
    has_mysqldb = True
except ImportError:
    import csv
    has_csv = True

has_psutil = False
try:
    import psutil
    has_psutil = True
except ImportError:
    pass

# configuration path for monitoring
current_location = os.getcwd()
CONFIG_PATH = os.path.join(current_location, "config.json")
try:
    MONITOR_CONFIGS = json.load(open(CONFIG_PATH))
except StandardError, e:
    print "Cannot locate configuration file for monitor"
    print e

_TWO_20 = float(2 ** 20)
_MYSQL = 0
_CSV = 1

    
class Monitor:

    def __init__(self, profiles, args, workflow):
        self.profiles = profiles
        self.args = args
        self.workflow = workflow

        self.cleaned_profiles = {}
        self.pe_process_map = {}

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
            print "Trying to store performance data to MySQL Database"
            db_config = MONITOR_CONFIGS["mysql_db_config"]

            conn = MySQLdb.connect(host=db_config["host"],
                           user=db_config["user"],
                           passwd=db_config["passwd"],
                           db=db_config["db"])

            with conn:
                success = self.record_wf_profile(wf_name, proc_num, writeto=_MYSQL, conn=conn)
                success = self.record_pe_profile(wf_name, writeto=_MYSQL, conn=conn) if success else success

            if success:
                print "Performance data stored to MySQL Database"
            else:
                print "Failed to record performance data of current workflow into MySQL Database"

        # store performance data into csv if no mysqldb module
        elif has_csv:
            print "Trying to write performance data to csv file"

            success = self.record_wf_profile(wf_name, proc_num, writeto=_CSV)
            success = self.record_pe_profile(wf_name, writeto=_CSV) if success else success

            if success:
                print "Performance data written to csv files"
            else:
                print "Failed to record performance data of current workflow into csv file"

        else:
            print "Failed to record data. CSV or MySQLdb module required."


    def record_wf_profile(self, wf_name, proc_num, writeto=_MYSQL, conn=None):
        wf_id = self.cleaned_profiles[wf_name].keys()[0]
        wf_id_bin = binascii.unhexlify(wf_id)
        wf_mapping = self.args.target
        wf_iter_num = self.args.iter
        wf_graph = os.path.join(current_location, MONITOR_CONFIGS["graph_profile_store"], wf_id)
        wf_memory_profile = os.path.join(current_location, MONITOR_CONFIGS["memory_profile_store"], wf_id) + ".dat"
        wf_total_time = self.cleaned_profiles[wf_name][wf_id]["exec"]
        wf_sub_time = self.cleaned_profiles[wf_name][wf_id]["submitted"]

        # convert graph to json in node link format
        from dispel4py.workflow_graph import drawDot, draw
        dot_data = draw(self.workflow)
        # store dot file to graph data path
        with open(wf_graph + ".dot", "w") as outfile:
            outfile.write(dot_data)

        # store png file if store_png set to true
        if MONITOR_CONFIGS.get("store_png", False):
            drawDot(self.workflow, wf_graph + ".png")

        if writeto == _MYSQL:
            cursor = conn.cursor()

            try:
                cursor.execute(
                    "INSERT INTO WorkflowProfiles("
                    "WF_SubmissionID, WF_Name, WF_Mapping, "
                    "WF_ProcessorNum, WF_IterationNum, WF_TotalTime, "
                    "WF_Submitted, WF_GraphDescription, WF_MemoryProfile) " +
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (wf_id_bin, wf_name, wf_mapping,
                     proc_num, wf_iter_num, wf_total_time,
                     wf_sub_time, wf_graph, wf_memory_profile)
                )
                conn.commit()
                return True

            except StandardError, e:
                print(cursor._last_executed)
                print e
                conn.rollback()
                return False

        elif writeto == _CSV:

            wf_output_path = os.path.join(home, MONITOR_CONFIGS["workflow_profile_store"], wf_id) + "-wf.csv"
            wf_headers = ["WF_SubmissionID", "WF_Name", "WF_Mapping",
                       "WF_ProcessorNum", "WF_IterationNum", "WF_GraphDescription",
                       "WF_MemoryProfile", "WF_TotalTime", "WF_Submitted"]

            wf_row = (wf_id, wf_name, wf_mapping,
                   proc_num, wf_iter_num, wf_graph,
                   wf_memory_profile, wf_total_time, wf_sub_time)
            try:
                with open(wf_output_path, 'w') as f:
                    f_csv = csv.writer(f)
                    f_csv.writerow(wf_headers)
                    f_csv.writerow(wf_row)

                print "Workflow benchmark written to disk"
                return True

            except StandardError, e:
                print e
                return False

    def record_pe_profile(self, wf_name, conn=None, writeto=_MYSQL):
        wf_id = self.cleaned_profiles[wf_name].keys()[0]

        # setup variables for mysql storage
        if writeto == _MYSQL:
            wf_id_stored = binascii.unhexlify(wf_id)
            cursor = conn.cursor()

            pei_insert_stat = (
                "INSERT INTO PEInstanceProfiles("
                "PEI_Rank, PEI_PEID, PEI_SubmissionID, "
                "PEI_TotalTime, PEI_ReadTime, PEI_WriteTime, "
                "PEI_ProcessTime, PEI_TerminateTime, PEI_InDataSize, "
                "PEI_OutDataSize, PEI_ReadRate, PEI_WriteRate) "
                "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )

        # setup variables for csv storage
        elif writeto == _CSV:
            wf_id_stored = wf_id
            pe_headers = ["PE_PEID", "PE_SubmissionID", "PE_TotalTime",
                          "PE_ReadTime", "PE_WriteTime", "PE_ProcessTime",
                          "PE_TerminateTime", "PE_InDataSize", "PE_OutDataSize",
                          "PE_InDataType", "PE_OutDataType", "PE_ReadRate",
                          "PE_WriteRate"]

            pei_headers = ["PEI_Rank", "PEI_PEID", "PEI_SubmissionID",
                           "PEI_TotalTime", "PEI_ReadTime", "PEI_WriteTime",
                           "PEI_ProcessTime", "PEI_TerminateTime", "PEI_InDataSize",
                           "PEI_OutDataSize", "PEI_ReadRate", "PEI_WriteRate"]

            pe_output_path = os.path.join(home, MONITOR_CONFIGS["workflow_profile_store"], wf_id) + "-pe.csv"

            pei_output_path = os.path.join(home, MONITOR_CONFIGS["workflow_profile_store"], wf_id) + "-pei.csv"

            with open(pe_output_path, "w") as f:
                pe_f_csv = csv.writer(f)
                pe_f_csv.writerow(pe_headers)

            with open(pei_output_path, "w") as f:
                pei_f_csv = csv.writer(f)
                pei_f_csv.writerow(pei_headers)

        for (pe_id, pe_p) in self.cleaned_profiles.items():
            if pe_id != wf_name:
                pei_total_sum = pei_read_sum = pei_write_sum = pei_process_sum = pei_terminate_sum = \
                    pe_indatasize = pe_outdatasize = sum_readrate = sum_writerate = 0

                pe_indatatype = pe_outdatatype = ""


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

                    pei_total_sum += pei_total
                    pei_read_sum += pei_read
                    pei_write_sum += pei_write
                    pei_process_sum += pei_process
                    pei_terminate_sum += pei_terminate
                    pe_indatasize += pei_indatasize
                    pe_outdatasize += pei_outdatasize
                    sum_readrate += pei_readrate if pei_readrate is not None else 0
                    sum_writerate += pei_writerate if pei_writerate is not None else 0

                    if pe_indatatype == "":
                        pe_indatatype = pei_p.get("indatatype", "")

                    if pe_outdatatype == "":
                        pe_outdatatype = pei_p.get("outdatatype", "")

                    insert_data = (pei_rank, pe_id, wf_id_stored,
                                   pei_total, pei_read, pei_write, pei_process,
                                   pei_terminate, pei_indatasize, pei_outdatasize,
                                   pei_readrate, pei_writerate)
                    pei_insert_data_sets.append(insert_data)

                instance_num = len(pe_p)

                pe_row = (pe_id, wf_id_stored, pei_total_sum / instance_num,
                          pei_read_sum / instance_num, pei_write_sum / instance_num, pei_process_sum / instance_num,
                          pei_terminate_sum / instance_num, pe_indatasize, pe_outdatasize,
                          pe_indatatype, pe_outdatatype, sum_readrate / len(pe_p),
                          sum_writerate / len(pe_p))

                if writeto == _MYSQL:
                    try:
                        cursor.execute(
                            "INSERT INTO PEProfiles("
                            "PE_PEID, PE_SubmissionID, PE_TotalTime, "
                            "PE_ReadTime, PE_WriteTime, PE_ProcessTime, "
                            "PE_TerminateTime, PE_InDataSize, PE_OutDataSize, "
                            "PE_InDataType, PE_OutDataType, PE_ReadRate, "
                            "PE_WriteRate) " +
                            "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            pe_row
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

                elif writeto == _CSV:
                    try:
                        with open(pe_output_path, "a") as f:
                            pe_f_csv = csv.writer(f)
                            pe_f_csv.writerow(pe_row)

                        with open(pei_output_path, "a") as f:
                            pei_f_csv = csv.writer(f)
                            pei_f_csv.writerows(pei_insert_data_sets)
                    except StandardError, e:
                        print e
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
                setattr(args[0], "_read_calls", getattr(args[0], "_read_calls") + 1)
            if hasattr(args[0], "_write_calls") and f.__name__ == "_write":
                setattr(args[0], "_write_calls", getattr(args[0], "_write_calls") + 1)
                # print ("Rank [%s] has written %s times." %(args[0].pe.rank, args[0]._write_calls))
        return f(*args, **kwargs)
    return wrapped


def memory_usage(proc=-1, interval=.1, timeout=None, timestamps=False, include_children=False, max_usage=False,
                 retval=False, stream=None, description=None):
    """
    :param proc: {int, string, tuple, subprocess.Popen}, optional
        The process to monitor. Can be given by an integer/string
        representing a PID, or by a tuple representing a Python function. The
        tuple contains three values (f, args, kw) and specifies to run the
        function f(*args, **kw).
        Set to -1 (default) for current process.
    :param interval: float, optional
        Interval at which measurements are collected.
    :param timeout: float, optional
        Maximum amount of time (in seconds) to wait before returning.
    :param timestamps: bool, optional
        if True, timestamps of memory usage measurement are collected as well.
    :param include_children: bool, optional
        if True, additionally monitor children processes of the process given.
    :param max_usage: bool, optional
        Only return the maximum memory usage (default False)
    :param retval: bool, optional
        For profiling python functions. Save the return value of the profiled
        function. Return value of memory_usage becomes a tuple:
        (mem_usage, retval)
    :param stream: File
        if stream is a File opened with write access, then results are written
        to this file instead of stored in memory and returned at the end of
        the subprocess. Useful for long-running processes.
        Implies timestamps=True.
    :param description: tuple, optional
        description of process being monitored. specified description will be
        added to output if stream set to True
    :return:
        mem_usage : list of floating-poing values
            memory usage, in MiB. It's length is always < timeout / interval
            if max_usage is given, returns the two elements maximum memory and
            number of measurements effectuated
        ret : return value of the profiled function
            Only returned if retval is set to True
    """

    if stream is not None:
        timestamps = True

    if not max_usage:
        ret = []
    else:
        ret = -1

    if timeout is not None:
        max_iter = int(timeout / interval)
    elif isinstance(proc, int):
        # external process and no timeout
        max_iter = 1
    else:
        # for a Python function wait until it finishes
        max_iter = float('inf')

    # description of process being profiled
    if description is not None:
        if isinstance(description, tuple):
            description = "\t".join(str(d) for d in description)

    if hasattr(proc, '__call__'):
        proc = (proc, (), {})

    if isinstance(proc, (list, tuple)):
        if len(proc) == 1:
            f, args, kw = (proc[0], (), {})
        elif len(proc) == 2:
            f, args, kw = (proc[0], proc[1], {})
        elif len(proc) == 3:
            f, args, kw = (proc[0], proc[1], proc[2])
        else:
            raise ValueError

        while True:
            child_conn, parent_conn = Pipe()  # this will store MemTimer's results
            p = MemTimer(os.getpid(), interval, child_conn, timestamps=timestamps, max_usage=max_usage,
                         include_children=include_children)
            p.start()
            parent_conn.recv()  # wait until we start getting memory
            returned = f(*args, **kw)
            parent_conn.send(0)  # finish timing
            ret = parent_conn.recv()
            n_measurements = parent_conn.recv()
            if retval:
                ret = ret, returned
            p.join(5 * interval)
            if n_measurements > 4 or interval < 1e-6:
                break
            interval /= 10.

    else:
        # external process
        if max_iter == -1:
            max_iter = 1
        counter = 0
        while counter < max_iter:
            counter += 1
            if not max_usage:
                mem_usage = _get_memory(proc, timestamps=timestamps, include_children=include_children)
                if stream is not None:
                    if description is not None:
                        mem_usg_output = "MEM\t{0:.6f}\t{1}\t{2}\n".format(mem_usage[0],
                                                                           time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                         time.localtime(mem_usage[1])),
                                                                           description)
                    else:
                        mem_usg_output = "MEM\t{0:.6f}\t{1}\n".format(mem_usage[0],
                                                                      time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                    time.localtime(mem_usage[1])))

                    stream.write(mem_usg_output)
                else:
                    ret.append(mem_usage)

                # Flush every 50 lines of memory footprints
                if counter % 50 == 0 and stream is not None:
                    stream.flush()
            else:
                ret = max([ret, _get_memory(proc, timestamps=timestamps, include_children=include_children)])
                # print ret

            time.sleep(interval)

        # Write and flush max memory usage
        if stream is not None and max_usage:
            if description is not None:
                mem_usg_output = "MEM\t{0:.6f}\t{1}\t{2}\n".format(ret[0],
                                                                   time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                 time.localtime(ret[1])),
                                                                   description)
            else:
                mem_usg_output = "MEM\t{0:.6f}\t{1}\n".format(ret[0],
                                                              time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                    time.localtime(ret[1])))

            stream.write(mem_usg_output)
            stream.flush()

    if stream:
        return None
    return ret


class MemTimer(Process):
    """
    Fetch memory consumption from over a time interval
    """

    def __init__(self, monitor_pid, interval, pipe, max_usage=False, *args, **kw):
        self.monitor_pid = monitor_pid
        self.interval = interval
        self.pipe = pipe
        self.cont = True
        self.max_usage = max_usage
        self.n_measurements = 1

        if "timestamps" in kw:
            self.timestamps = kw["timestamps"]
            del kw["timestamps"]
        else:
            self.timestamps = False
        if "include_children" in kw:
            self.include_children = kw["include_children"]
            del kw["include_children"]
        else:
            self.include_children = False
        # get baseline memory usage
        self.mem_usage = [
            _get_memory(self.monitor_pid, timestamps=self.timestamps, include_children=self.include_children)]
        super(MemTimer, self).__init__(*args, **kw)

    def run(self):
        self.pipe.send(0)  # we're ready
        stop = False
        while True:
            cur_mem = _get_memory(self.monitor_pid, timestamps=self.timestamps, include_children=self.include_children)
            if not self.max_usage:
                self.mem_usage.append(cur_mem)
            else:
                self.mem_usage[0] = max(cur_mem, self.mem_usage[0])
            self.n_measurements += 1
            if stop:
                break
            stop = self.pipe.poll(self.interval)
            # do one more iteration

        self.pipe.send(self.mem_usage)
        self.pipe.send(self.n_measurements)


def _get_memory(pid, timestamps=False, include_children=False):
    """
    :param pid: int
        id of process to monitor, -1 implies current process
    :param timestamps: bool, optional
        if True, timestamps of memory usage measurement are collected as well.
    :param include_children: bool, optional

    :return:
        mem : memory usage in MiB
        time.time() : current timestamp
    """
    # monitor current process
    if pid == -1:
        pid = os.getpid()

    # cross-platform but but requires psutil
    if has_psutil:
        process = psutil.Process(pid)
        try:
            # avoid using get_memory_info since it does not exists
            # in psutil > 2.0 and accessing it will cause exception
            meminfo_attr = 'memory_info' if hasattr(process, 'memory_info') else 'get_memory_info'
            mem = getattr(process, meminfo_attr)()[0] / _TWO_20
            if include_children:
                for p in process.get_children(recursive=True):
                    mem += getattr(process, meminfo_attr)()[0] / _TWO_20
            if timestamps:
                return mem, time.time()
            else:
                return mem
        except psutil.AccessDenied:
            pass
            # continue and try to get this from ps

    if os.name == 'posix':
        if include_children:
            raise NotImplementedError('The psutil module is required when to'
                                      ' monitor memory usage of children'
                                      ' processes')
        warnings.warn("psutil module not found. memory_profiler will be slow")
        # memory usage in MiB
        # this should work on both Mac and Linux
        # subprocess.check_output appeared in 2.7, using Popen
        # for backwards compatibility
        out = subprocess.Popen(['ps', 'v', '-p', str(pid)], stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        try:
            vsz_index = out[0].split().index(b'RSS')
            mem = float(out[1].split()[vsz_index]) / 1024
            if timestamps:
                return mem, time.time()
            else:
                return mem
        except:
            if timestamps:
                return -1, time.time()
            else:
                return -1
    else:
        raise NotImplementedError('The psutil module is required for non-unix '
                                  'platforms')

