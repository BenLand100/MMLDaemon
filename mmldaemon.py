#!/usr/bin/python

"""
 *  Copyright 2010 by Benjamin J. Land (a.k.a. BenLand100)
 *
 *  This file is part of the MMLDaemon project.
 *
 *  MMLDaemon is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  MMLDaemon is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with MMLDaemon. If not, see <http://www.gnu.org/licenses/>.
"""

"""
Basically you run this file, and that starts the Daemon (MMLDaemon) which 
listens on the given port for Clients to connect (via sockets, obviously).
When Clients connect, they use a protocol loosely defined in the code to
request things from the Daemon. Something simple would be a command like
`spawn` that would create a Worker (MMLWorker) which runs in a subprocess
and then Daemon will return the Worker's PID. Using that PID, the Client 
can request the Daemon to tell the Worker to do things. The Daemon can 
also send data from the Worker back to the Clients across the same socket.
The Daemon communicates with the Worker over a Python Pipe (its pretty 
cool, really). A Worker basically consists of a ScriptRunner subclass 
(none yet created) that will abstract out all the things a Worker can do,
and allow many different bindings all running from multiple processes
controlled by the Daemon. If a Client disconnects from the Daemon, its
spawned workers are terminated.
"""

from struct import *
from socket import *
from ctypes import *
from threading import *
from sys import argv, exit
from time import sleep
from multiprocessing import Process, Pipe

class ScriptRunner(object):
    """Superclass for various types of Script running modules."""
    def __init__(self,debug):
        """Sets the stdout/stderr method, should be invoked by subclasses."""
        self._debug = debug
    def start(self,program,finished):
        """Override with a method to start scripts."""
        self._debug('Program: ' + program)
        self._debug('Successfully Compiled')
        self._debug('Successfully Executed')
        finished()
    def stop(self):
        """Override with a method to stop scripts."""
        self._debug('Terminating Script')
    def pause(self):
        """Override with a method to pause scripts."""
        self._debug('Pausing Script')

class PYScriptRunner(ScriptRunner):
    """Should implement whatever is necessary to run a python script."""
    def __init__(self,debug):
        ScriptRunner.__init__(self,debug)
        self._debug('Greetings from the Python Script Engine')

class PSScriptRunner(ScriptRunner):
    """Should implement whatever is necessary to run a pascalscript script."""
    def __init__(self,debug):
        ScriptRunner.__init__(self,debug)
        self._debug('Greetings from the PascalScript Script Engine')

class CPascalScriptRunner(ScriptRunner):
    """Implements a CPascal script component."""
    CErrorHandler = CFUNCTYPE(None,c_int,c_int,c_char_p,c_bool)
    CPreCompiler = CFUNCTYPE(c_bool,c_char_p,c_char_p)
    CWriteln = CFUNCTYPE(None,c_char_p)
    def __init__(self,debug):
        """Loads the native lib and sets up an interpreter instance."""
        ScriptRunner.__init__(self,debug)
        self._debug('Initializing CPascal')
        self._dll = CDLL('./libcpascal.so')
        print 'Loaded module:',self._dll
        self._dll.interp_mem(None,None,None)
        self._precomphandler = CPascalScriptRunner.CPreCompiler(self._precomp)
        self._errorhandler = CPascalScriptRunner.CErrorHandler(self._error)
        self._interp = self._dll.interp_init(self._precomphandler,self._errorhandler)
        print 'CPascal interpreter initilized'
    def __del__(self):
        """Frees the native stuff."""
        self._dll.interp_free(self._interp)
    def _precomp(self,value,args):
        """Precompiler callback from the native lib."""
        self._debug('Precompiler: {'+value+','+args+'}')
        return True
    def _error(self,line,pos,err,runtime):
        """Error callback from the native lib."""
        self._debug(('Runtime ' if runtime else '') +'Error <'+str(line)+','+str(pos)+'> ' + err)
    def _run(self,finished):
        """Threaded script execution entry point."""
        self._dll.interp_run(self._interp)
        finished()
    def start(self,program,finished):
        """Starts the program in a new thread so as to not block other signals."""
        self._dll.interp_set(self._interp,program)
        self._dll.interp_comp(self._interp)
        self._thread = Thread(target=self._run,args=(finished,))
        self._thread.start()
    def stop(self):
        """Stop is not implemented."""
        self._debug('CPascal does not implement STOP')
    def pause(self):
        """Pause is not implemented."""
        self._debug('CPascal does not implement PASUSE')

def worker(pipe,runner):
    """The main method for the worker subprocesses."""
    debug = lambda s: pipe.send(('debug',s))
    comp = runner(debug)
    funcdict = {'stop':comp.stop,'start':lambda program:comp.start(program,lambda:pipe.send(('finished',None))),'pause':comp.pause}
    while True:
        (key,val) = pipe.recv()
        if key in funcdict:
            apply(funcdict[key],val)

class MMLWorker(object):
    """Represents the Daemon's connection to the subprocess"""
    def __init__(self,runner):
        """Creates and initalizes a subprocess and its connections."""
        self.pipe, pipe = Pipe()
        self.proc = Process(target=worker, args=(pipe,runner))
        self.proc.start();
        self.pid = self.proc.pid
    def __del__(self):
        """Ensures the subprocess is correctly garbage collected."""
        self.pipe.close();
        self.proc.terminate();
    def pump(self,block=False):
        """Returns a key,val pair from the subprocess, or None,None."""
        key,val = None,None
        if block:
            (key,val) = self.pipe.recv()
        elif self.pipe.poll():
            (key,val) = self.pipe.recv()
        return key,val
    def stop(self):
        """Sends the stop signal to the subprocess."""
        self.pipe.send(('stop',()))
    def pause(self):
        """Sends the pause signal to the subprocess."""
        self.pipe.send(('pause',()))
    def start(self,program):
        """Sends the start signal to the subprocess."""
        self.pipe.send(('start',(program,)))

#Client/Daemon protocol: [command] [args] 
#commands are a one byte int
#args can be: type, pid, string
#type is a one byte int
#pid is a four byte int
#string is a four byte int length followed by string data

MMLD_SPAWN      = 0 #C->D | [type]                  #client requests daemon for a new worker of a type
MMLD_WORKER     = 1 #D->C | [pid]                   #daemon responds with the new worker's pid
MMLD_START      = 2 #C->D | [pid] [string]          #client signals worker to start a program with a string arg
MMLD_STOP       = 3 #C->D | [pid]                   #client signals the worker to stop
MMLD_PAUSE      = 4 #C->D | [pid]                   #client signals the worker to pause
MMLD_DISCONNECT = 7 #C->D |                         #client notifies daemon it is disconnecting
MMLD_ERROR      = 8 #D->C | [string]                #daemon encountered an error and must terminate the connection
MMLD_KILL       = 9 #C->D | [pid]                   #client requests a worker process be terminated
MMLD_FINISHED   =10 #D->C | [pid]                   #daemon notifies client that a script has terminated
MMLD_DEBUG      =11 #D->C | [pid] [string]          #daemon sends client the debug from a worker

#type codes for ScriptRunner types
MMLD_PS         = 0 #pascalscript
MMLD_PY         = 1 #python
MMLD_CPAS       = 2 #cpascal

#map types to class objects
ScriptRunners = {MMLD_PS:PSScriptRunner,MMLD_PY:PYScriptRunner,MMLD_CPAS:CPascalScriptRunner}

class MMLDaemon(object):
    """Listens on a socket and controls a pool of subprocess workers."""
    def __init__(self,port):
        self.socket = socket(AF_INET,SOCK_STREAM)
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        self.socket.setblocking(0)
        self.socket.bind(('', port))
        self.socket.listen(5)
        self.sockets = [] #sockets 
        self.clients = {} #pid -> socket
        self.pool = {} #pid -> worker
    def __del__(self):
        """Used to notify any remaining clients that the end is nigh."""
        for conn in self.sockets:
            self.disconnect(conn,'Daemon is shutting down')
    def pump(self):
        """Couples the socket and subprocesses; does as much as it can, then returns; call often."""
        try:
            while True:
                conn,addr = self.socket.accept()
                conn.setblocking(0)
                self.sockets.append(conn)
                print 'Client Connected',addr
        except error:
            pass
        for conn in self.sockets:
            try:
                while True: #when out of data, recv will raise an error
                    (code,) = unpack('=B',conn.recv(1))
                    try:
                        if code == MMLD_SPAWN:
                            (runner,) = unpack('=B',conn.recv(1))
                            pid = self.spawn(conn,ScriptRunners[runner])
                            conn.send(pack('=Bi',MMLD_WORKER,pid))
                        elif code == MMLD_START:
                            (pid,size) = unpack('=ii',conn.recv(8))
                            (program,) = unpack('='+str(size)+'s',conn.recv(size))
                            self.pool[pid].start(program)
                        elif code == MMLD_STOP:
                            (pid,) = unpack('=i',conn.recv(4))
                            self.pool[pid].stop()
                        elif code == MMLD_PAUSE:
                            (pid,) = unpack('=i',conn.recv(4))
                            self.pool[pid].pause()
                        elif code == MMLD_DISCONNECT:
                            self.disconnect(conn)
                        elif code == MMLD_KILL:
                            (pid,) = unpack('=i',conn.recv(4))
                            self.kill(pid)
                        else:
                            raise Exception('Unknown command: ' + str(code))
                    except error:
                        raise Exception('Syntax error: ' + str(code))
            except error:
                pass
            except Exception as ex:
                self.disconnect(conn,str(ex))
        for pid in self.pool:
            conn = self.clients[pid]
            worker = self.pool[pid]
            key,value = worker.pump()
            while key:
                print '<'+str(pid)+'>',key,'=>',value
                if key == 'debug':
                    conn.send(pack('=Bii'+str(len(value))+'s',MMLD_DEBUG,pid,len(value),value))
                elif key == 'finished':
                    conn.send(pack('=Bi',MMLD_FINISHED,pid))
                key,value = worker.pump()
        return True
    def spawn(self,conn,runner):
        """Returns the pid of a newly created worked subprocess."""
        worker = MMLWorker(runner)
        self.pool[worker.pid] = worker
        self.clients[worker.pid] = conn
        print 'Spawned worker subprocess:', worker.pid
        return worker.pid
    def kill(self,pid):
        """Terminates and frees a subprocess."""
        del self.clients[pid]
        del self.pool[pid]
        print 'Terminated worker subprocess:', pid
    def disconnect(self,conn,why=None):
        """Disconnects a client."""
        try:  
            conn.send(pack('=Bi'+str(len(why))+'s',MMLD_ERROR,len(why),why))
        except:
            pass
        pids = []
        for pid in self.clients:
            if self.clients[pid] == conn: pids.append(pid)
        for pid in pids:
            self.kill(pid)
        self.sockets.remove(conn)
        conn.close()
        print 'Client Terminated'
        
if __name__ == '__main__':
    port = 8000
    try:
        if len(argv) > 2:
            raise Exception('Too many arguments')
        elif len(argv) == 2:
            port = int(argv[1])
    except:
        print 'Usage: mmldaemon.py [port='+str(port)+']'
        exit(1)
    daemon = MMLDaemon(port)
    while daemon.pump():
        sleep(0.1)
    del daemon
