import os
import sys
import platform

MYPATH = os.path.abspath(__file__)
MYDIR = os.path.dirname(MYPATH)
DEVMODEMSG = '*** DEVELOPER MODE: setting PATH, PYTHONPATH and LD_LIBRARY_PATH ***'

if MYDIR.endswith('src') and \
   os.path.exists(os.path.join(MYDIR, '.libs')) and \
   os.path.exists(os.path.join(MYDIR, 'pybind')):

    if platform.system() == "Darwin":
        lib_path_var = "DYLD_LIBRARY_PATH"
    else:
        lib_path_var = "LD_LIBRARY_PATH"

    py_binary = os.environ.get("PYTHON", "python")
    MYLIBPATH = os.path.join(MYDIR, '.libs')
    execv_cmd = ['python']
    if 'CEPH_DBG' in os.environ:
        execv_cmd += ['-mpdb']
    if lib_path_var in os.environ:
        if MYLIBPATH not in os.environ[lib_path_var]:
            os.environ[lib_path_var] += ':' + MYLIBPATH
            print >> sys.stderr, DEVMODEMSG
            os.execvp(py_binary, execv_cmd + sys.argv)
    else:
        os.environ[lib_path_var] = MYLIBPATH
        print >> sys.stderr, DEVMODEMSG
        os.execvp(py_binary, execv_cmd + sys.argv)
    sys.path.insert(0, os.path.join(MYDIR, 'pybind'))
    if os.environ.has_key('PATH') and MYDIR not in os.environ['PATH']:
        os.environ['PATH'] += ':' + MYDIR

import sys
import json
from rados import Rados

rados = Rados(conffile='', rados_id='admin')
rados.connect()
ioctx = rados.open_ioctx("rbd")

script = """
function run(input, output)
    output:append('{ret}' .. ',' .. cls.clock());
end;
cls.register(run);
"""

cmd = {
    "prefix" : "osd pool set",
    "pool" : "rbd",
    "var": "lua_class",
}


counter = 0
while True:
    cmd.update({"val": script.format(ret = counter)})
    print json.dumps(cmd)
    ret, buf, errs = rados.mon_command(json.dumps(cmd), '', timeout=30)
    print ret, buf, errs, counter
    counter += 1
    break
