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

val = "default lua class value"
if len(sys.argv) > 1 and len(sys.argv[1]) > 0:
    val = sys.argv[1]

#
# Start with something simple. We'll have a call below that sends a lua script
# to the monitor. The monitor will toss it in a OSDMap incremental update.
# Then when the map is applied at the OSDs the new Lua script will become
# active (no versioning right now).
#
# In init_op_flags we'll handle the Lua case similar to how its setup right
# now. Basically we assume that cls_lua exists and clients use the exec
# function in a similar way. However, we'll add a special method called like
# exec_internal (or whatever). Then when clients call this method the Lua
# script in the OSD map will be substitued for what is normally a script
# provided as a paraemter to the exec call.
#
# This all seems to be minimally invasive and the quickest path forward.
#
# Clients would have to go through the API to modify the scripts which will
# enforce any rules we want. We'll ignore the fact that cls_lua also lets you
# do whatever you want as well. That is a separate, non-research issue.
#
# This is all just a first step. This should be sufficient to start
# benchmarking CORFU implementations as well as benchmarking change
# propogation costs.
#

script = """
function read3(input, output)
  size = cls.stat()
  bl = cls.read(0, size)
  output:append(bl:str())
  cls.log(input:str())
end

cls.register(read3)
"""

cmd = {
    "prefix" : "osd pool set",
    "pool" : "rbd",
    "var": "lua_class",
    "val": script,
}

ret, buf, errs = rados.mon_command(json.dumps(cmd), '', timeout=30)

print ioctx.write_full("asdf", "helloasdf111")

exe = {
    "script": script,
    "handler": "read2",
    "input": "hmmmmm"
}

x = ioctx.execute("asdf", "lua", "read3", "wtf")
print x, len(x)

print ret, buf, errs

