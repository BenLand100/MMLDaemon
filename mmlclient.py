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

from socket import *
from struct import *
from time import sleep

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
MMLD_CPAS       = 2 #python

socket = socket(AF_INET,SOCK_STREAM)
socket.connect(('localhost', 8000))

socket.send(pack('=BB',MMLD_SPAWN,MMLD_CPAS))
while True:
    try:
        (code,) = unpack('=B',socket.recv(1))
        if code == MMLD_WORKER:
            (pid,) = unpack('=i',socket.recv(4))
            #begin test code            
            print 'Worker Created:',pid
            program = 'program new; var x: integer; begin x:= 1 + 2 end.'
            print 'Starting a Script:',pid
            socket.send(pack('=Bii'+str(len(program))+'s',MMLD_START,pid,len(program),program))
            #socket.send(pack('=Bi',MMLD_KILL,pid))
            #end test code
        elif code == MMLD_DEBUG:
            (pid,size) = unpack('=ii',socket.recv(8))
            (msg,) = unpack('='+str(size)+'s',socket.recv(size))
            print '<'+str(pid)+'>', msg
        elif code == MMLD_ERROR:
            (size,) = unpack('=i',socket.recv(4))
            (why,) = unpack('='+str(size)+'s',socket.recv(size))
            raise Exception(why)
        elif code == MMLD_FINISHED:
            (pid,) = unpack('=i',socket.recv(4))
            print 'Script Terminated:',pid
            raise Exception('Finished')
        else:
            raise Exception('Unknown Daemon Command: ' + str(code))
    except error:
        raise
