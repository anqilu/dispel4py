"""Profile the memory usage of a Python program"""

# .. we'll use this to pass it to the child script ..
_clean_globals = globals().copy()

__version__ = '0.32'

_CMD_USAGE = "python -m memory_profiler script_file.py"

import time
import sys
import os
import pdb
import warnings
import linecache
import inspect
import subprocess
import logging

from multiprocessing import Process, Pipe

_TWO_20 = float(2 ** 20)

has_psutil = False

# .. get available packages ..
try:
    import psutil

    has_psutil = True
except ImportError:
    pass


def _get_memory(pid, timestamps=False, include_children=False):
    # .. only for current process and only on unix..
    if pid == -1:
        pid = os.getpid()

    # .. cross-platform but but requires psutil ..
    if has_psutil:
        process = psutil.Process(pid)
        try:
            # avoid useing get_memory_info since it does not exists 
            # in psutil > 2.0 and accessing it will cause exception.
            meminfo_attr = 'memory_info' if hasattr(process, 'memory_info') else 'get_memory_info'
            mem = getattr(process, meminfo_attr)()[0] / _TWO_20
            if include_children:
                for p in process.get_children(recursive=True):
                    mem += getattr(process, meminfo_attr)()[0] / _TWO_20
            if timestamps:
                return (mem, time.time())
            else:
                return mem
        except psutil.AccessDenied:
            pass
            # continue and try to get this from ps

    # .. scary stuff ..
    if os.name == 'posix':
        if include_children:
            raise NotImplementedError('The psutil module is required when to'
                                      ' monitor memory usage of children'
                                      ' processes')
        warnings.warn("psutil module not found. memory_profiler will be slow")
        # ..
        # .. memory usage in MiB ..
        # .. this should work on both Mac and Linux ..
        # .. subprocess.check_output appeared in 2.7, using Popen ..
        # .. for backwards compatibility ..
        out = subprocess.Popen(['ps', 'v', '-p', str(pid)], stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        try:
            vsz_index = out[0].split().index(b'RSS')
            mem = float(out[1].split()[vsz_index]) / 1024
            if timestamps:
                return (mem, time.time())
            else:
                return mem
        except:
            if timestamps:
                return (-1, time.time())
            else:
                return -1
    else:
        raise NotImplementedError('The psutil module is required for non-unix '
                                  'platforms')


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


def memory_usage(proc=-1, interval=.1, timeout=None, timestamps=False, include_children=False, max_usage=False,
                 retval=False, stream=None, description=None):
    """
    Return the memory usage of a process or piece of code

    Parameters
    ----------
    proc : {int, string, tuple, subprocess.Popen}, optional
        The process to monitor. Can be given by an integer/string
        representing a PID, by a Popen object or by a tuple
        representing a Python function. The tuple contains three
        values (f, args, kw) and specifies to run the function
        f(*args, **kw).
        Set to -1 (default) for current process.

    interval : float, optional
        Interval at which measurements are collected.

    timeout : float, optional
        Maximum amount of time (in seconds) to wait before returning.

    max_usage : bool, optional
        Only return the maximum memory usage (default False)

    retval : bool, optional
        For profiling python functions. Save the return value of the profiled
        function. Return value of memory_usage becomes a tuple:
        (mem_usage, retval)

    timestamps : bool, optional
        if True, timestamps of memory usage measurement are collected as well.

    stream : File
        if stream is a File opened with write access, then results are written
        to this file instead of stored in memory and returned at the end of
        the subprocess. Useful for long-running processes.
        Implies timestamps=True.

    Returns
    -------
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
    elif isinstance(proc, subprocess.Popen):
        # external process, launched from Python
        line_count = 0
        while True:
            if not max_usage:
                mem_usage = _get_memory(proc.pid, timestamps=timestamps, include_children=include_children)
                if stream is not None:
                    stream.write("MEM {0:.6f} {1:.4f}\n".format(*mem_usage))
                else:
                    ret.append(mem_usage)
            else:
                ret = max([ret, _get_memory(proc.pid, include_children=include_children)])
            time.sleep(interval)
            line_count += 1
            # flush every 50 lines. Make 'tail -f' usable on profile file
            if line_count > 50:
                line_count = 0
                if stream is not None:
                    stream.flush()
            if timeout is not None:
                max_iter -= 1
                if max_iter == 0:
                    break
            if proc.poll() is not None:
                break
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


# ..
# .. utility functions for line-by-line ..


def _find_script(script_name):
    """ Find the script.

    If the input is not a file, then $PATH will be searched.
    """
    if os.path.isfile(script_name):
        return script_name
    path = os.getenv('PATH', os.defpath).split(os.pathsep)
    for folder in path:
        if not folder:
            continue
        fn = os.path.join(folder, script_name)
        if os.path.isfile(fn):
            return fn

    sys.stderr.write('Could not find script {0}\n'.format(script_name))
    raise SystemExit(1)


class _TimeStamperCM(object):
    """Time-stamping context manager."""

    def __init__(self, timestamps):
        self._timestamps = timestamps

    def __enter__(self):
        self._timestamps.append(_get_memory(os.getpid(), timestamps=True))

    def __exit__(self, *args):
        self._timestamps.append(_get_memory(os.getpid(), timestamps=True))


class TimeStamper:
    """ A profiler that just records start and end execution times for
    any decorated function.
    """

    def __init__(self):
        self.functions = {}

    def __call__(self, func=None, precision=None):
        if func is not None:
            if not hasattr(func, "__call__"):
                raise ValueError("Value must be callable")

            self.add_function(func)
            f = self.wrap_function(func)
            f.__module__ = func.__module__
            f.__name__ = func.__name__
            f.__doc__ = func.__doc__
            f.__dict__.update(getattr(func, '__dict__', {}))
            return f
        else:
            def inner_partial(f):
                return self.__call__(f, precision=precision)

            return inner_partial

    def timestamp(self, name="<block>"):
        """Returns a context manager for timestamping a block of code."""
        # Make a fake function
        func = lambda x: x
        func.__module__ = ""
        func.__name__ = name
        self.add_function(func)
        timestamps = []
        self.functions[func].append(timestamps)
        # A new object is required each time, since there can be several
        # nested context managers.
        return _TimeStamperCM(timestamps)

    def add_function(self, func):
        if not func in self.functions:
            self.functions[func] = []

    def wrap_function(self, func):
        """ Wrap a function to timestamp it.
        """

        def f(*args, **kwds):
            # Start time
            timestamps = [_get_memory(os.getpid(), timestamps=True)]
            self.functions[func].append(timestamps)
            try:
                result = func(*args, **kwds)
            finally:
                # end time
                timestamps.append(_get_memory(os.getpid(), timestamps=True))
            return result

        return f

    def show_results(self, stream=None):
        if stream is None:
            stream = sys.stdout

        for func, timestamps in self.functions.items():
            function_name = "%s.%s" % (func.__module__, func.__name__)
            for ts in timestamps:
                stream.write("FUNC %s %.4f %.4f %.4f %.4f\n" % ((function_name,) + ts[0] + ts[1]))


class LineProfiler(object):
    """ A profiler that records the amount of memory for each line """

    def __init__(self, **kw):
        self.code_map = {}
        self.enable_count = 0
        self.max_mem = kw.get('max_mem', None)
        self.prevline = None
        self.include_children = kw.get('include_children', False)

    def __call__(self, func=None, precision=1):
        if func is not None:
            self.add_function(func)
            f = self.wrap_function(func)
            f.__module__ = func.__module__
            f.__name__ = func.__name__
            f.__doc__ = func.__doc__
            f.__dict__.update(getattr(func, '__dict__', {}))
            return f
        else:
            def inner_partial(f):
                return self.__call__(f, precision=precision)

            return inner_partial

    def add_code(self, code, toplevel_code=None):
        if code not in self.code_map:
            self.code_map[code] = {}

            for subcode in filter(inspect.iscode, code.co_consts):
                self.add_code(subcode)

    def add_function(self, func):
        """ Record line profiling information for the given Python function.
        """
        try:
            # func_code does not exist in Python3
            code = func.__code__
        except AttributeError:
            warnings.warn("Could not extract a code object for the object %r" % func)
        else:
            self.add_code(code)

    def wrap_function(self, func):
        """ Wrap a function to profile it.
        """

        def f(*args, **kwds):
            self.enable_by_count()
            try:
                result = func(*args, **kwds)
            finally:
                self.disable_by_count()
            return result

        return f

    def run(self, cmd):
        """ Profile a single executable statement in the main namespace.
        """
        # TODO: can this be removed ?
        import __main__
        main_dict = __main__.__dict__
        return self.runctx(cmd, main_dict, main_dict)

    def runctx(self, cmd, globals, locals):
        """ Profile a single executable statement in the given namespaces.
        """
        self.enable_by_count()
        try:
            exec (cmd, globals, locals)
        finally:
            self.disable_by_count()
        return self

    def enable_by_count(self):
        """ Enable the profiler if it hasn't been enabled before.
        """
        if self.enable_count == 0:
            self.enable()
        self.enable_count += 1

    def disable_by_count(self):
        """ Disable the profiler if the number of disable requests matches the
        number of enable requests.
        """
        if self.enable_count > 0:
            self.enable_count -= 1
            if self.enable_count == 0:
                self.disable()

    def trace_memory_usage(self, frame, event, arg):
        """Callback for sys.settrace"""
        if (event in ('call', 'line', 'return') and frame.f_code in self.code_map):
            if event != 'call':
                # "call" event just saves the lineno but not the memory
                mem = _get_memory(-1, include_children=self.include_children)
                # if there is already a measurement for that line get the max
                old_mem = self.code_map[frame.f_code].get(self.prevline, 0)
                self.code_map[frame.f_code][self.prevline] = max(mem, old_mem)
            self.prevline = frame.f_lineno

        if self._original_trace_function is not None:
            (self._original_trace_function)(frame, event, arg)

        return self.trace_memory_usage

    def trace_max_mem(self, frame, event, arg):
        # run into PDB as soon as memory is higher than MAX_MEM
        if event in ('line', 'return') and frame.f_code in self.code_map:
            c = _get_memory(-1)
            if c >= self.max_mem:
                t = ('Current memory {0:.2f} MiB exceeded the maximum'
                     ''.format(c) + 'of {0:.2f} MiB\n'.format(self.max_mem))
                sys.stdout.write(t)
                sys.stdout.write('Stepping into the debugger \n')
                frame.f_lineno -= 2
                p = pdb.Pdb()
                p.quitting = False
                p.stopframe = frame
                p.returnframe = None
                p.stoplineno = frame.f_lineno - 3
                p.botframe = None
                return p.trace_dispatch

        if self._original_trace_function is not None:
            (self._original_trace_function)(frame, event, arg)

        return self.trace_max_mem

    def __enter__(self):
        self.enable_by_count()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable_by_count()

    def enable(self):
        self._original_trace_function = sys.gettrace()
        if self.max_mem is not None:
            sys.settrace(self.trace_max_mem)
        else:
            sys.settrace(self.trace_memory_usage)

    def disable(self):
        sys.settrace(self._original_trace_function)


def show_results(prof, stream=None, precision=1):
    if stream is None:
        stream = sys.stdout
    template = '{0:>6} {1:>12} {2:>12}   {3:<}'

    for code in prof.code_map:
        lines = prof.code_map[code]
        if not lines:
            # .. measurements are empty ..
            continue
        filename = code.co_filename
        if filename.endswith((".pyc", ".pyo")):
            filename = filename[:-1]
        stream.write('Filename: ' + filename + '\n\n')
        if not os.path.exists(filename):
            stream.write('ERROR: Could not find file ' + filename + '\n')
            if any([filename.startswith(k) for k in ("ipython-input", "<ipython-input")]):
                print("NOTE: %mprun can only be used on functions defined in "
                      "physical files, and not in the IPython environment.")
            continue
        all_lines = linecache.getlines(filename)
        sub_lines = inspect.getblock(all_lines[code.co_firstlineno - 1:])
        linenos = range(code.co_firstlineno, code.co_firstlineno + len(sub_lines))

        header = template.format('Line #', 'Mem usage', 'Increment', 'Line Contents')
        stream.write(header + '\n')
        stream.write('=' * len(header) + '\n')

        mem_old = lines[min(lines.keys())]
        float_format = '{0}.{1}f'.format(precision + 4, precision)
        template_mem = '{0:' + float_format + '} MiB'
        for line in linenos:
            mem = ''
            inc = ''
            if line in lines:
                mem = lines[line]
                inc = mem - mem_old
                mem_old = mem
                mem = template_mem.format(mem)
                inc = template_mem.format(inc)
            stream.write(template.format(line, mem, inc, all_lines[line - 1]))
        stream.write('\n\n')


def profile(func=None, stream=None, precision=1):
    """
    Decorator that will run the function and print a line-by-line profile
    """
    if func is not None:
        def wrapper(*args, **kwargs):
            prof = LineProfiler()
            val = prof(func)(*args, **kwargs)
            show_results(prof, stream=stream, precision=precision)
            return val

        return wrapper
    else:
        def inner_wrapper(f):
            return profile(f, stream=stream, precision=precision)

        return inner_wrapper


class LogFile(object):
    """File-like object to log text using the `logging` module and the log report can be customised."""

    def __init__(self, name=None, reportIncrementFlag=False):
        """
        :param name: name of the logger module
               reportIncrementFlag: This must be set to True if only the steps with memory increments are to be reported

        :type self: object
              name: string
              reportIncrementFlag: bool
        """
        self.logger = logging.getLogger(name)
        self.reportIncrementFlag = reportIncrementFlag

    def write(self, msg, level=logging.INFO):
        if self.reportIncrementFlag:
            if "MiB" in msg and float(msg.split("MiB")[1].strip()) > 0:
                self.logger.log(level, msg)
            elif msg.__contains__("Filename:") or msg.__contains__("Line Contents"):
                self.logger.log(level, msg)
        else:
            self.logger.log(level, msg)

    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()
